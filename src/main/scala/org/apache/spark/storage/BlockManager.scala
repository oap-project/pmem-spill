/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.io._
import java.lang.ref.{ReferenceQueue => JReferenceQueue, WeakReference}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.Collections
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, TimeUnit}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}
import scala.util.control.NonFatal

import com.codahale.metrics.{MetricRegistry, MetricSet}
import com.google.common.cache.CacheBuilder
import com.intel.oap.common.unsafe.PersistentMemoryPlatform

import org.apache.commons.io.IOUtils

import org.apache.spark._
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Network
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.metrics.source.Source
import org.apache.spark.network._
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle._
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.util.TransportConf
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{ShuffleManager, ShuffleWriteMetricsReporter}
import org.apache.spark.storage.memory._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/* Class for returning a fetched block and associated metrics. */
private[spark] class BlockResult(
    val data: Iterator[Any],
    val readMethod: DataReadMethod.Value,
    val bytes: Long)

/**
 * Abstracts away how blocks are stored and provides different ways to read the underlying block
 * data. Callers should call [[dispose()]] when they're done with the block.
 */
private[spark] trait BlockData {

  def toInputStream(): InputStream

  /**
   * Returns a Netty-friendly wrapper for the block's data.
   *
   * Please see `ManagedBuffer.convertToNetty()` for more details.
   */
  def toNetty(): Object

  def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer

  def toByteBuffer(): ByteBuffer

  def size: Long

  def dispose(): Unit

}

private[spark] class ByteBufferBlockData(
    val buffer: ChunkedByteBuffer,
    val shouldDispose: Boolean) extends BlockData {

  override def toInputStream(): InputStream = buffer.toInputStream(dispose = false)

  override def toNetty(): Object = buffer.toNetty

  override def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    buffer.copy(allocator)
  }

  override def toByteBuffer(): ByteBuffer = buffer.toByteBuffer

  override def size: Long = buffer.size

  override def dispose(): Unit = {
    if (shouldDispose) {
      buffer.dispose()
    }
  }

}

private[spark] class HostLocalDirManager(
    futureExecutionContext: ExecutionContext,
    cacheSize: Int,
    externalBlockStoreClient: ExternalBlockStoreClient,
    host: String,
    externalShuffleServicePort: Int) extends Logging {

  private val executorIdToLocalDirsCache =
    CacheBuilder
      .newBuilder()
      .maximumSize(cacheSize)
      .build[String, Array[String]]()

  private[spark] def getCachedHostLocalDirs()
      : scala.collection.Map[String, Array[String]] = executorIdToLocalDirsCache.synchronized {
    import scala.collection.JavaConverters._
    return executorIdToLocalDirsCache.asMap().asScala
  }

  private[spark] def getHostLocalDirs(
      executorIds: Array[String])(
      callback: Try[java.util.Map[String, Array[String]]] => Unit): Unit = {
    val hostLocalDirsCompletable = new CompletableFuture[java.util.Map[String, Array[String]]]
    externalBlockStoreClient.getHostLocalDirs(
      host,
      externalShuffleServicePort,
      executorIds,
      hostLocalDirsCompletable)
    hostLocalDirsCompletable.whenComplete { (hostLocalDirs, throwable) =>
      if (hostLocalDirs != null) {
        callback(Success(hostLocalDirs))
        executorIdToLocalDirsCache.synchronized {
          executorIdToLocalDirsCache.putAll(hostLocalDirs)
        }
      } else {
        callback(Failure(throwable))
      }
    }
  }
}

/**
 * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 *
 * Note that [[initialize()]] must be called before the BlockManager is usable.
 */
private[spark] class BlockManager(
    executorId: String,
    rpcEnv: RpcEnv,
    val master: BlockManagerMaster,
    val serializerManager: SerializerManager,
    val conf: SparkConf,
    memoryManager: MemoryManager,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    val blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    externalBlockStoreClient: Option[ExternalBlockStoreClient])
  extends BlockDataManager with BlockEvictionHandler with Logging {

  // same as `conf.get(config.SHUFFLE_SERVICE_ENABLED)`
  private[spark] val externalShuffleServiceEnabled: Boolean = externalBlockStoreClient.isDefined

  val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER
  var memExtensionEnabled = conf.getBoolean("spark.memory.pmem.extension.enabled", false)

  var numaNodeId = conf.getInt("spark.executor.numa.id", -1)
  val pmemInitialPaths = conf.get("spark.memory.pmem.initial.path", "").split(",")
  val pmemInitialSize = conf.getSizeAsBytes("spark.memory.pmem.initial.size", 0L)
  val pmemMode = conf.get("spark.memory.pmem.mode", "AppDirect")
  val numNum = conf.getInt("spark.yarn.numa.num", 2)

  if (pmemMode.equals("AppDirect")) {
    if (!isDriver && pmemInitialPaths.size >= 1) {
      if (numaNodeId == -1) {
        numaNodeId = executorId.toInt
      }
      val path = pmemInitialPaths(numaNodeId % 2)
      val initPath = path + File.separator + s"executor_${executorId}" + File.pathSeparator
      val file = new File(initPath)
      if (file.exists() && file.isFile) {
        file.delete()
      }

      if (!file.exists()) {
        file.mkdirs()
      }

      require(file.isDirectory(), "PMem directory is required for initialization")
      PersistentMemoryPlatform.initialize(initPath, pmemInitialSize, 0)
      logInfo(s"Intel Optane PMem initialized with path: ${initPath}, size: ${pmemInitialSize} ")
    }
  } else if (pmemMode.equals("KMemDax")) {
    if (!isDriver) {
      if (numaNodeId == -1) {
        numaNodeId = (executorId.toInt + 1) % 2
      }
      val daxNodeId = numaNodeId + numNum
      PersistentMemoryPlatform.setNUMANode(String.valueOf(daxNodeId), String.valueOf(numaNodeId))
      PersistentMemoryPlatform.initialize()
    }
  }

  private val remoteReadNioBufferConversion =
    conf.get(Network.NETWORK_REMOTE_READ_NIO_BUFFER_CONVERSION)

  private[spark] val subDirsPerLocalDir = conf.get(config.DISKSTORE_SUB_DIRECTORIES)

  val diskBlockManager = {
    // Only perform cleanup if an external service is not serving our shuffle files.
    val deleteFilesOnStop =
      !externalShuffleServiceEnabled || executorId == SparkContext.DRIVER_IDENTIFIER
    new DiskBlockManager(conf, deleteFilesOnStop)
  }

  // Visible for testing
  private[storage] val blockInfoManager = new BlockInfoManager

  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  // Actual storage of where blocks are kept
  private[spark] val memoryStore =
    new MemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)
  private[spark] val diskStore = new DiskStore(conf, diskBlockManager, securityManager)
  memoryManager.setMemoryStore(memoryStore)

  // Note: depending on the memory manager, `maxMemory` may actually vary over time.
  // However, since we use this only for reporting and logging, what we actually want here is
  // the absolute maximum value that `maxMemory` can ever possibly reach. We may need
  // to revisit whether reporting this value as the "max" is intuitive to the user.
  private val maxOnHeapMemory = memoryManager.maxOnHeapStorageMemory
  private val maxOffHeapMemory = memoryManager.maxOffHeapStorageMemory

  private val externalShuffleServicePort = StorageUtils.externalShuffleServicePort(conf)

  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' blocks. This is either an external service, or just the
  // standard BlockTransferService to directly connect to other Executors.
  private[spark] val blockStoreClient = externalBlockStoreClient.getOrElse(blockTransferService)

  // Max number of failures before this block manager refreshes the block locations from the driver
  private val maxFailuresBeforeLocationRefresh =
    conf.get(config.BLOCK_FAILURES_BEFORE_LOCATION_REFRESH)

  private val slaveEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker))

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  // Field related to peer block managers that are necessary for block replication
  @volatile private var cachedPeers: Seq[BlockManagerId] = _
  private val peerFetchLock = new Object
  private var lastPeerFetchTimeNs = 0L

  private var blockReplicationPolicy: BlockReplicationPolicy = _

  // A DownloadFileManager used to track all the files of remote blocks which are above the
  // specified memory threshold. Files will be deleted automatically based on weak reference.
  // Exposed for test
  private[storage] val remoteBlockTempFileManager =
    new BlockManager.RemoteBlockDownloadFileManager(this)
  private val maxRemoteBlockToMem = conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)

  var hostLocalDirManager: Option[HostLocalDirManager] = None

  /**
   * Abstraction for storing blocks from bytes, whether they start in memory or on disk.
   *
   * @param blockSize the decrypted size of the block
   */
  private[spark] abstract class BlockStoreUpdater[T](
      blockSize: Long,
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean,
      keepReadLock: Boolean) {

    /**
     *  Reads the block content into the memory. If the update of the block store is based on a
     *  temporary file this could lead to loading the whole file into a ChunkedByteBuffer.
     */
    protected def readToByteBuffer(): ChunkedByteBuffer

    protected def blockData(): BlockData

    protected def saveToDiskStore(): Unit

    private def saveDeserializedValuesToMemoryStore(inputStream: InputStream): Boolean = {
      try {
        val values = serializerManager.dataDeserializeStream(blockId, inputStream)(classTag)
        memoryStore.putIteratorAsValues(blockId, values, classTag) match {
          case Right(_) => true
          case Left(iter) =>
            // If putting deserialized values in memory failed, we will put the bytes directly
            // to disk, so we don't need this iterator and can close it to free resources
            // earlier.
            iter.close()
            false
        }
      } finally {
        IOUtils.closeQuietly(inputStream)
      }
    }

    private def saveSerializedValuesToMemoryStore(bytes: ChunkedByteBuffer): Boolean = {
      val memoryMode = level.memoryMode
      memoryStore.putBytes(blockId, blockSize, memoryMode, () => {
        if (memoryMode == MemoryMode.OFF_HEAP && bytes.chunks.exists(!_.isDirect)) {
          bytes.copy(Platform.allocateDirectBuffer)
        } else {
          bytes
        }
      })
    }

    /**
     * Put the given data according to the given level in one of the block stores, replicating
     * the values if necessary.
     *
     * If the block already exists, this method will not overwrite it.
     *
     * If keepReadLock is true, this method will hold the read lock when it returns (even if the
     * block already exists). If false, this method will hold no locks when it returns.
     *
     * @return true if the block was already present or if the put succeeded, false otherwise.
     */
     def save(): Boolean = {
      doPut(blockId, level, classTag, tellMaster, keepReadLock) { info =>
        val startTimeNs = System.nanoTime()

        // Since we're storing bytes, initiate the replication before storing them locally.
        // This is faster as data is already serialized and ready to send.
        val replicationFuture = if (level.replication > 1) {
          Future {
            // This is a blocking action and should run in futureExecutionContext which is a cached
            // thread pool.
            replicate(blockId, blockData(), level, classTag)
          }(futureExecutionContext)
        } else {
          null
        }
        if (level.useMemory) {
          // Put it in memory first, even if it also has useDisk set to true;
          // We will drop it to disk later if the memory store can't hold it.
          val putSucceeded = if (level.deserialized) {
            saveDeserializedValuesToMemoryStore(blockData().toInputStream())
          } else {
            saveSerializedValuesToMemoryStore(readToByteBuffer())
          }
          if (!putSucceeded && level.useDisk) {
            logWarning(s"Persisting block $blockId to disk instead.")
            saveToDiskStore()
          }
        } else if (level.useDisk) {
          saveToDiskStore()
        }
        val putBlockStatus = getCurrentBlockStatus(blockId, info)
        val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
        if (blockWasSuccessfullyStored) {
          // Now that the block is in either the memory or disk store,
          // tell the master about it.
          info.size = blockSize
          if (tellMaster && info.tellMaster) {
            reportBlockStatus(blockId, putBlockStatus)
          }
          addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        }
        logDebug(s"Put block ${blockId} locally took ${Utils.getUsedTimeNs(startTimeNs)}")
        if (level.replication > 1) {
          // Wait for asynchronous replication to finish
          try {
            ThreadUtils.awaitReady(replicationFuture, Duration.Inf)
          } catch {
            case NonFatal(t) =>
              throw new Exception("Error occurred while waiting for replication to finish", t)
          }
        }
        if (blockWasSuccessfullyStored) {
          None
        } else {
          Some(blockSize)
        }
      }.isEmpty
    }
  }

  /**
   * Helper for storing a block from bytes already in memory.
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   */
  private case class ByteBufferBlockStoreUpdater[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      bytes: ChunkedByteBuffer,
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false)
    extends BlockStoreUpdater[T](bytes.size, blockId, level, classTag, tellMaster, keepReadLock) {

    override def readToByteBuffer(): ChunkedByteBuffer = bytes

    /**
     * The ByteBufferBlockData wrapper is not disposed of to avoid releasing buffers that are
     * owned by the caller.
     */
    override def blockData(): BlockData = new ByteBufferBlockData(bytes, false)

    override def saveToDiskStore(): Unit = diskStore.putBytes(blockId, bytes)

  }

  /**
   * Helper for storing a block based from bytes already in a local temp file.
   */
  private[spark] case class TempFileBasedBlockStoreUpdater[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      tmpFile: File,
      blockSize: Long,
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false)
    extends BlockStoreUpdater[T](blockSize, blockId, level, classTag, tellMaster, keepReadLock) {

    override def readToByteBuffer(): ChunkedByteBuffer = {
      val allocator = level.memoryMode match {
        case MemoryMode.ON_HEAP => ByteBuffer.allocate _
        case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
        case MemoryMode.PMEM => PersistentMemoryPlatform.allocateVolatileDirectBuffer _
      }
      blockData().toChunkedByteBuffer(allocator)
    }

    override def blockData(): BlockData = diskStore.getBytes(tmpFile, blockSize)

    override def saveToDiskStore(): Unit = diskStore.moveFileToBlock(tmpFile, blockSize, blockId)

    override def save(): Boolean = {
      val res = super.save()
      tmpFile.delete()
      res
    }

  }

  /**
   * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
   *
   * This method initializes the BlockTransferService and BlockStoreClient, registers with the
   * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
   * service if configured.
   */
  def initialize(appId: String): Unit = {
    blockTransferService.init(this)
    externalBlockStoreClient.foreach { blockStoreClient =>
      blockStoreClient.init(appId)
    }
    blockReplicationPolicy = {
      val priorityClass = conf.get(config.STORAGE_REPLICATION_POLICY)
      val clazz = Utils.classForName(priorityClass)
      val ret = clazz.getConstructor().newInstance().asInstanceOf[BlockReplicationPolicy]
      logInfo(s"Using $priorityClass for block replication policy")
      ret
    }

    val id =
      BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)

    val idFromMaster = master.registerBlockManager(
      id,
      diskBlockManager.localDirsString,
      maxOnHeapMemory,
      maxOffHeapMemory,
      slaveEndpoint)

    blockManagerId = if (idFromMaster != null) idFromMaster else id

    shuffleServerId = if (externalShuffleServiceEnabled) {
      logInfo(s"external shuffle service port = $externalShuffleServicePort")
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }

    // Register Executors' configuration with the local shuffle service, if one should exist.
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }

    hostLocalDirManager =
      if (conf.get(config.SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED) &&
          !conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
        externalBlockStoreClient.map { blockStoreClient =>
          new HostLocalDirManager(
            futureExecutionContext,
            conf.get(config.STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE),
            blockStoreClient,
            blockManagerId.host,
            externalShuffleServicePort)
        }
      } else {
        None
      }

    logInfo(s"Initialized BlockManager: $blockManagerId")
  }

  def shuffleMetricsSource: Source = {
    import BlockManager._

    if (externalShuffleServiceEnabled) {
      new ShuffleMetricsSource("ExternalShuffle", blockStoreClient.shuffleMetrics())
    } else {
      new ShuffleMetricsSource("NettyBlockTransfer", blockStoreClient.shuffleMetrics())
    }
  }

  private def registerWithExternalShuffleServer(): Unit = {
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirsString,
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.getClass.getName)

    val MAX_ATTEMPTS = conf.get(config.SHUFFLE_REGISTRATION_MAX_ATTEMPTS)
    val SLEEP_TIME_SECS = 5

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // Synchronous and will throw an exception if we cannot connect.
        blockStoreClient.asInstanceOf[ExternalBlockStoreClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000L)
        case NonFatal(e) =>
          throw new SparkException("Unable to register with external shuffle server due to : " +
            e.getMessage, e)
      }
    }
  }

  /**
   * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   *
   * This function deliberately fails silently if the master returns false (indicating that
   * the slave needs to re-register). The error condition will be detected again by the next
   * heart beat attempt or new block registration and another try to re-register all blocks
   * will be made then.
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfoManager.size} blocks to the master.")
    for ((blockId, info) <- blockInfoManager.entries) {
      val status = getCurrentBlockStatus(blockId, info)
      if (info.tellMaster && !tryToReportBlockStatus(blockId, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * Re-register with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block manager indicates that we were not registered.
   *
   * Note that this method must be called without any BlockInfo locks held.
   */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo(s"BlockManager $blockManagerId re-registering with master")
    master.registerBlockManager(blockManagerId, diskBlockManager.localDirsString, maxOnHeapMemory,
      maxOffHeapMemory, slaveEndpoint)
    reportAllBlocks()
  }

  /**
   * Re-register with the master sometime soon.
   */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
  }

  /**
   * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      try {
        ThreadUtils.awaitReady(task, Duration.Inf)
      } catch {
        case NonFatal(t) =>
          throw new Exception("Error occurred while waiting for async. reregistration", t)
      }
    }
  }

  override def getHostLocalShuffleData(
      blockId: BlockId,
      dirs: Array[String]): ManagedBuffer = {
    shuffleManager.shuffleBlockResolver.getBlockData(blockId, Some(dirs))
  }

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   */
  override def getLocalBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {
      shuffleManager.shuffleBlockResolver.getBlockData(blockId)
    } else {
      getLocalBytes(blockId) match {
        case Some(blockData) =>
          new BlockManagerManagedBuffer(blockInfoManager, blockId, blockData, true)
        case None =>
          // If this block manager receives a request for a block that it doesn't have then it's
          // likely that the master has outdated block statuses for this block. Therefore, we send
          // an RPC so that this block is marked as being unavailable from this block manager.
          reportBlockStatus(blockId, BlockStatus.empty)
          throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

  /**
   * Put the block locally, using the given storage level.
   *
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   */
  override def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean = {
    putBytes(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), level)(classTag)
  }

  override def putBlockDataAsStream(
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_]): StreamCallbackWithID = {
    val (_, tmpFile) = diskBlockManager.createTempLocalBlock()
    val channel = new CountingWritableChannel(
      Channels.newChannel(serializerManager.wrapForEncryption(new FileOutputStream(tmpFile))))
    logTrace(s"Streaming block $blockId to tmp file $tmpFile")
    new StreamCallbackWithID {

      override def getID: String = blockId.name

      override def onData(streamId: String, buf: ByteBuffer): Unit = {
        while (buf.hasRemaining) {
          channel.write(buf)
        }
      }

      override def onComplete(streamId: String): Unit = {
        logTrace(s"Done receiving block $blockId, now putting into local blockManager")
        // Note this is all happening inside the netty thread as soon as it reads the end of the
        // stream.
        channel.close()
        val blockSize = channel.getCount
        TempFileBasedBlockStoreUpdater(blockId, level, classTag, tmpFile, blockSize).save()
      }

      override def onFailure(streamId: String, cause: Throwable): Unit = {
        // the framework handles the connection itself, we just need to do local cleanup
        channel.close()
        tmpFile.delete()
      }
    }
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * NOTE: This is mainly for testing.
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfoManager.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      BlockStatus(info.level, memSize = memSize, diskSize = diskSize)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the disk block manager (that the block manager
   * may not know of).
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    // The `toArray` is necessary here in order to force the list to be materialized so that we
    // don't try to serialize a lazy iterator when responding to client requests.
    (blockInfoManager.entries.map(_._1) ++ diskBlockManager.getAllBlocks())
      .filter(filter)
      .toArray
      .toSeq
  }

  /**
   * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   *
   * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
   * it is still valid). This ensures that update in master will compensate for the increase in
   * memory on slave.
   */
  private def reportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit = {
    val needReregister = !tryToReportBlockStatus(blockId, status, droppedMemorySize)
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // Re-registering will report our new block for free.
      asyncReregister()
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
   * Actually send a UpdateBlockInfo message. Returns the master's response,
   * which will be true if the block was successfully recorded and false if
   * the slave needs to re-register.
   */
  private def tryToReportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean = {
    val storageLevel = status.storageLevel
    val inMemSize = Math.max(status.memSize, droppedMemorySize)
    val onDiskSize = status.diskSize
    master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
  }

  /**
   * Return the updated storage status of the block with the given ID. More specifically, if
   * the block is dropped from memory and possibly added to disk, return the new storage level
   * and the updated in-memory and on-disk sizes.
   */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      info.level match {
        case null =>
          BlockStatus.empty
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem  || onDisk) level.replication else 1
          val storageLevel = StorageLevel(
            useDisk = onDisk,
            useMemory = inMem,
            useOffHeap = level.useOffHeap,
            deserialized = deserialized,
            replication = replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          BlockStatus(storageLevel, memSize, diskSize)
      }
    }
  }

  /**
   * Get locations of an array of blocks.
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeNs = System.nanoTime()
    val locations = master.getLocations(blockIds).toArray
    logDebug(s"Got multiple block location in ${Utils.getUsedTimeNs(startTimeNs)}")
    locations
  }

  /**
   * Cleanup code run in response to a failed local read.
   * Must be called while holding a read lock on the block.
   */
  private def handleLocalReadFailure(blockId: BlockId): Nothing = {
    releaseLock(blockId)
    // Remove the missing block so that its unavailability is reported to the driver
    removeBlock(blockId)
    throw new SparkException(s"Block $blockId was not found even though it's read-locked")
  }

  /**
   * Get block from local block manager as an iterator of Java objects.
   */
  def getLocalValues(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    blockInfoManager.lockForReading(blockId) match {
      case None =>
        logDebug(s"Block $blockId was not found")
        None
      case Some(info) =>
        val level = info.level
        logDebug(s"Level for block $blockId is $level")
        val taskContext = Option(TaskContext.get())
        if (level.useMemory && memoryStore.contains(blockId)) {
          val iter: Iterator[Any] = if (level.deserialized) {
            memoryStore.getValues(blockId).get
          } else {
            serializerManager.dataDeserializeStream(
              blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)
          }
          // We need to capture the current taskId in case the iterator completion is triggered
          // from a different thread which does not have TaskContext set; see SPARK-18406 for
          // discussion.
          val ci = CompletionIterator[Any, Iterator[Any]](iter, {
            releaseLock(blockId, taskContext)
          })
          Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
        } else if (level.useDisk && diskStore.contains(blockId)) {
          val diskData = diskStore.getBytes(blockId)
          val iterToReturn: Iterator[Any] = {
            if (level.deserialized) {
              val diskValues = serializerManager.dataDeserializeStream(
                blockId,
                diskData.toInputStream())(info.classTag)
              maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
            } else {
              val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
                .map { _.toInputStream(dispose = false) }
                .getOrElse { diskData.toInputStream() }
              serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
            }
          }
          val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, {
            releaseLockAndDispose(blockId, diskData, taskContext)
          })
          Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
        } else {
          handleLocalReadFailure(blockId)
        }
    }
  }

  /**
   * Get block from the local block manager as serialized bytes.
   */
  def getLocalBytes(blockId: BlockId): Option[BlockData] = {
    logDebug(s"Getting local block $blockId as bytes")
    assert(!blockId.isShuffle, s"Unexpected ShuffleBlockId $blockId")
    blockInfoManager.lockForReading(blockId).map { info => doGetLocalBytes(blockId, info) }
  }

  /**
   * Get block from the local block manager as serialized bytes.
   *
   * Must be called while holding a read lock on the block.
   * Releases the read lock upon exception; keeps the read lock upon successful return.
   */
  private def doGetLocalBytes(blockId: BlockId, info: BlockInfo): BlockData = {
    val level = info.level
    logDebug(s"Level for block $blockId is $level")
    // In order, try to read the serialized bytes from memory, then from disk, then fall back to
    // serializing in-memory objects, and, finally, throw an exception if the block does not exist.
    if (level.deserialized) {
      // Try to avoid expensive serialization by reading a pre-serialized copy from disk:
      if (level.useDisk && diskStore.contains(blockId)) {
        // Note: we purposely do not try to put the block back into memory here. Since this branch
        // handles deserialized blocks, this block may only be cached in memory as objects, not
        // serialized bytes. Because the caller only requested bytes, it doesn't make sense to
        // cache the block's deserialized objects since that caching may not have a payoff.
        diskStore.getBytes(blockId)
      } else if (level.useMemory && memoryStore.contains(blockId)) {
        // The block was not found on disk, so serialize an in-memory copy:
        new ByteBufferBlockData(serializerManager.dataSerializeWithExplicitClassTag(
          blockId, memoryStore.getValues(blockId).get, info.classTag), true)
      } else {
        handleLocalReadFailure(blockId)
      }
    } else {  // storage level is serialized
      if (level.useMemory && memoryStore.contains(blockId)) {
        new ByteBufferBlockData(memoryStore.getBytes(blockId).get, false)
      } else if (level.useDisk && diskStore.contains(blockId)) {
        val diskData = diskStore.getBytes(blockId)
        maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
          .map(new ByteBufferBlockData(_, false))
          .getOrElse(diskData)
      } else {
        handleLocalReadFailure(blockId)
      }
    }
  }

  /**
   * Get block from remote block managers.
   *
   * This does not acquire a lock on this block in this JVM.
   */
  private[spark] def getRemoteValues[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val ct = implicitly[ClassTag[T]]
    getRemoteBlock(blockId, (data: ManagedBuffer) => {
      val values =
        serializerManager.dataDeserializeStream(blockId, data.createInputStream())(ct)
      new BlockResult(values, DataReadMethod.Network, data.size)
    })
  }

  /**
   * Get the remote block and transform it to the provided data type.
   *
   * If the block is persisted to the disk and stored at an executor running on the same host then
   * first it is tried to be accessed using the local directories of the other executor directly.
   * If the file is successfully identified then tried to be transformed by the provided
   * transformation function which expected to open the file. If there is any exception during this
   * transformation then block access falls back to fetching it from the remote executor via the
   * network.
   *
   * @param blockId identifies the block to get
   * @param bufferTransformer this transformer expected to open the file if the block is backed by a
   *                          file by this it is guaranteed the whole content can be loaded
   * @tparam T result type
   */
  private[spark] def getRemoteBlock[T](
      blockId: BlockId,
      bufferTransformer: ManagedBuffer => T): Option[T] = {
    logDebug(s"Getting remote block $blockId")
    require(blockId != null, "BlockId is null")

    // Because all the remote blocks are registered in driver, it is not necessary to ask
    // all the slave executors to get block status.
    val locationsAndStatusOption = master.getLocationsAndStatus(blockId, blockManagerId.host)
    if (locationsAndStatusOption.isEmpty) {
      logDebug(s"Block $blockId is unknown by block manager master")
      None
    } else {
      val locationsAndStatus = locationsAndStatusOption.get
      val blockSize = locationsAndStatus.status.diskSize.max(locationsAndStatus.status.memSize)

      locationsAndStatus.localDirs.flatMap { localDirs =>
        val blockDataOption =
          readDiskBlockFromSameHostExecutor(blockId, localDirs, locationsAndStatus.status.diskSize)
        val res = blockDataOption.flatMap { blockData =>
          try {
            Some(bufferTransformer(blockData))
          } catch {
            case NonFatal(e) =>
              logDebug("Block from the same host executor cannot be opened: ", e)
              None
          }
        }
        logInfo(s"Read $blockId from the disk of a same host executor is " +
          (if (res.isDefined) "successful." else "failed."))
        res
      }.orElse {
        fetchRemoteManagedBuffer(blockId, blockSize, locationsAndStatus).map(bufferTransformer)
      }
    }
  }

  private def preferExecutors(locations: Seq[BlockManagerId]): Seq[BlockManagerId] = {
    val (executors, shuffleServers) = locations.partition(_.port != externalShuffleServicePort)
    executors ++ shuffleServers
  }

  /**
   * Return a list of locations for the given block, prioritizing the local machine since
   * multiple block managers can share the same host, followed by hosts on the same rack.
   *
   * Within each of the above listed groups (same host, same rack and others) executors are
   * preferred over the external shuffle service.
   */
  private[spark] def sortLocations(locations: Seq[BlockManagerId]): Seq[BlockManagerId] = {
    val locs = Random.shuffle(locations)
    val (preferredLocs, otherLocs) = locs.partition(_.host == blockManagerId.host)
    val orderedParts = blockManagerId.topologyInfo match {
      case None => Seq(preferredLocs, otherLocs)
      case Some(_) =>
        val (sameRackLocs, differentRackLocs) = otherLocs.partition {
          loc => blockManagerId.topologyInfo == loc.topologyInfo
        }
        Seq(preferredLocs, sameRackLocs, differentRackLocs)
    }
    orderedParts.map(preferExecutors).reduce(_ ++ _)
  }

  /**
   * Fetch the block from remote block managers as a ManagedBuffer.
   */
  private def fetchRemoteManagedBuffer(
      blockId: BlockId,
      blockSize: Long,
      locationsAndStatus: BlockManagerMessages.BlockLocationsAndStatus): Option[ManagedBuffer] = {
    // If the block size is above the threshold, we should pass our FileManger to
    // BlockTransferService, which will leverage it to spill the block; if not, then passed-in
    // null value means the block will be persisted in memory.
    val tempFileManager = if (blockSize > maxRemoteBlockToMem) {
      remoteBlockTempFileManager
    } else {
      null
    }
    var runningFailureCount = 0
    var totalFailureCount = 0
    val locations = sortLocations(locationsAndStatus.locations)
    val maxFetchFailures = locations.size
    var locationIterator = locations.iterator
    while (locationIterator.hasNext) {
      val loc = locationIterator.next()
      logDebug(s"Getting remote block $blockId from $loc")
      val data = try {
        val buf = blockTransferService.fetchBlockSync(loc.host, loc.port, loc.executorId,
          blockId.toString, tempFileManager)
        if (blockSize > 0 && buf.size() == 0) {
          throw new IllegalStateException("Empty buffer received for non empty block")
        }
        buf
      } catch {
        case NonFatal(e) =>
          runningFailureCount += 1
          totalFailureCount += 1

          if (totalFailureCount >= maxFetchFailures) {
            // Give up trying anymore locations. Either we've tried all of the original locations,
            // or we've refreshed the list of locations from the master, and have still
            // hit failures after trying locations from the refreshed list.
            logWarning(s"Failed to fetch block after $totalFailureCount fetch failures. " +
              s"Most recent failure cause:", e)
            return None
          }

          logWarning(s"Failed to fetch remote block $blockId " +
            s"from $loc (failed attempt $runningFailureCount)", e)

          // If there is a large number of executors then locations list can contain a
          // large number of stale entries causing a large number of retries that may
          // take a significant amount of time. To get rid of these stale entries
          // we refresh the block locations after a certain number of fetch failures
          if (runningFailureCount >= maxFailuresBeforeLocationRefresh) {
            locationIterator = sortLocations(master.getLocations(blockId)).iterator
            logDebug(s"Refreshed locations from the driver " +
              s"after ${runningFailureCount} fetch failures.")
            runningFailureCount = 0
          }

          // This location failed, so we retry fetch from a different one by returning null here
          null
      }

      if (data != null) {
        // If the ManagedBuffer is a BlockManagerManagedBuffer, the disposal of the
        // byte buffers backing it may need to be handled after reading the bytes.
        // In this case, since we just fetched the bytes remotely, we do not have
        // a BlockManagerManagedBuffer. The assert here is to ensure that this holds
        // true (or the disposal is handled).
        assert(!data.isInstanceOf[BlockManagerManagedBuffer])
        return Some(data)
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }

  /**
   * Reads the block from the local directories of another executor which runs on the same host.
   */
  private[spark] def readDiskBlockFromSameHostExecutor(
      blockId: BlockId,
      localDirs: Array[String],
      blockSize: Long): Option[ManagedBuffer] = {
    val file = ExecutorDiskUtils.getFile(localDirs, subDirsPerLocalDir, blockId.name)
    if (file.exists()) {
      val mangedBuffer = securityManager.getIOEncryptionKey() match {
        case Some(key) =>
          // Encrypted blocks cannot be memory mapped; return a special object that does decryption
          // and provides InputStream / FileRegion implementations for reading the data.
          new EncryptedManagedBuffer(
            new EncryptedBlockData(file, blockSize, conf, key))

        case _ =>
          val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
          new FileSegmentManagedBuffer(transportConf, file, 0, file.length)
      }
      Some(mangedBuffer)
    } else {
      None
    }
  }

  /**
   * Get block from remote block managers as serialized bytes.
   */
  def getRemoteBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    getRemoteBlock(blockId, (data: ManagedBuffer) => {
      // SPARK-24307 undocumented "escape-hatch" in case there are any issues in converting to
      // ChunkedByteBuffer, to go back to old code-path.  Can be removed post Spark 2.4 if
      // new path is stable.
      if (remoteReadNioBufferConversion) {
        new ChunkedByteBuffer(data.nioByteBuffer())
      } else {
        ChunkedByteBuffer.fromManagedBuffer(data)
      }
    })
  }

  /**
   * Get a block from the block manager (either local or remote).
   *
   * This acquires a read lock on the block if the block was stored locally and does not acquire
   * any locks if the block was fetched from a remote block manager. The read lock will
   * automatically be freed once the result's `data` iterator is fully consumed.
   */
  def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val local = getLocalValues(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemoteValues[T](blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  /**
   * Downgrades an exclusive write lock to a shared read lock.
   */
  def downgradeLock(blockId: BlockId): Unit = {
    blockInfoManager.downgradeLock(blockId)
  }

  /**
   * Release a lock on the given block with explicit TaskContext.
   * The param `taskContext` should be passed in case we can't get the correct TaskContext,
   * for example, the input iterator of a cached RDD iterates to the end in a child
   * thread.
   */
  def releaseLock(blockId: BlockId, taskContext: Option[TaskContext] = None): Unit = {
    val taskAttemptId = taskContext.map(_.taskAttemptId())
    // SPARK-27666. When a task completes, Spark automatically releases all the blocks locked
    // by this task. We should not release any locks for a task that is already completed.
    if (taskContext.isDefined && taskContext.get.isCompleted) {
      logWarning(s"Task ${taskAttemptId.get} already completed, not releasing lock for $blockId")
    } else {
      blockInfoManager.unlock(blockId, taskAttemptId)
    }
  }

  /**
   * Registers a task with the BlockManager in order to initialize per-task bookkeeping structures.
   */
  def registerTask(taskAttemptId: Long): Unit = {
    blockInfoManager.registerTask(taskAttemptId)
  }

  /**
   * Release all locks for the given task.
   *
   * @return the blocks whose locks were released.
   */
  def releaseAllLocksForTask(taskAttemptId: Long): Seq[BlockId] = {
    blockInfoManager.releaseAllLocksForTask(taskAttemptId)
  }

  /**
   * Retrieve the given block if it exists, otherwise call the provided `makeIterator` method
   * to compute the block, persist it, and return its values.
   *
   * @return either a BlockResult if the block was successfully cached, or an iterator if the block
   *         could not be cached.
   */
  def getOrElseUpdate[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      makeIterator: () => Iterator[T]): Either[BlockResult, Iterator[T]] = {
    // Attempt to read the block from local or remote storage. If it's present, then we don't need
    // to go through the local-get-or-put path.
    get[T](blockId)(classTag) match {
      case Some(block) =>
        return Left(block)
      case _ =>
        // Need to compute the block.
    }
    // Initially we hold no locks on this block.
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
      case None =>
        // doPut() didn't hand work back to us, so the block already existed or was successfully
        // stored. Therefore, we now hold a read lock on the block.
        val blockResult = getLocalValues(blockId).getOrElse {
          // Since we held a read lock between the doPut() and get() calls, the block should not
          // have been evicted, so get() not returning the block indicates some internal error.
          releaseLock(blockId)
          throw new SparkException(s"get() failed for block $blockId even though we held a lock")
        }
        // We already hold a read lock on the block from the doPut() call and getLocalValues()
        // acquires the lock again, so we need to call releaseLock() here so that the net number
        // of lock acquisitions is 1 (since the caller will only call release() once).
        releaseLock(blockId)
        Left(blockResult)
      case Some(iter) =>
        // The put failed, likely because the data was too large to fit in memory and could not be
        // dropped to disk. Therefore, we need to pass the input iterator back to the caller so
        // that they can decide what to do with the values (e.g. process them without caching).
       Right(iter)
    }
  }

  /**
   * @return true if the block was stored or false if an error occurred.
   */
  def putIterator[T: ClassTag](
      blockId: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(values != null, "Values is null")
    doPutIterator(blockId, () => values, level, implicitly[ClassTag[T]], tellMaster) match {
      case None =>
        true
      case Some(iter) =>
        // Caller doesn't care about the iterator values, so we can close the iterator here
        // to free resources earlier
        iter.close()
        false
    }
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
   */
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetricsReporter): DiskBlockObjectWriter = {
    val syncWrites = conf.get(config.SHUFFLE_SYNC)
    new DiskBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,
      syncWrites, writeMetrics, blockId)
  }

  /**
    * A short circuited method to get a PMem writer that can write data directly to PMem.
    * The Block will be appended to the PMem stream specified by filename. Callers should handle
    * error cases.
    */
  def getPMemWriter(
                     blockId: BlockId,
                     file: File,
                     serializerInstance: SerializerInstance,
                     bufferSize: Int,
                     writeMetrics: ShuffleWriteMetricsReporter): PMemBlockObjectWriter = {
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new PMemBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,
      syncWrites, writeMetrics, blockId)
  }

  /**
   * Put a new block of serialized bytes to the block manager.
   *
   * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
   * so may corrupt or change the data stored by the `BlockManager`.
   *
   * @return true if the block was stored or false if an error occurred.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      bytes: ChunkedByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(bytes != null, "Bytes is null")
    val blockStoreUpdater =
      ByteBufferBlockStoreUpdater(blockId, level, implicitly[ClassTag[T]], bytes, tellMaster)
    blockStoreUpdater.save()
  }

  /**
   * Helper method used to abstract common code from [[BlockStoreUpdater.save()]]
   * and [[doPutIterator()]].
   *
   * @param putBody a function which attempts the actual put() and returns None on success
   *                or Some on failure.
   */
  private def doPut[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_],
      tellMaster: Boolean,
      keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")

    val putBlockInfo = {
      val newInfo = new BlockInfo(level, classTag, tellMaster)
      if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {
        newInfo
      } else {
        logWarning(s"Block $blockId already exists on this machine; not re-adding it")
        if (!keepReadLock) {
          // lockNewBlockForWriting returned a read lock on the existing block, so we must free it:
          releaseLock(blockId)
        }
        return None
      }
    }

    val startTimeNs = System.nanoTime()
    var exceptionWasThrown: Boolean = true
    val result: Option[T] = try {
      val res = putBody(putBlockInfo)
      exceptionWasThrown = false
      if (res.isEmpty) {
        // the block was successfully stored
        if (keepReadLock) {
          blockInfoManager.downgradeLock(blockId)
        } else {
          blockInfoManager.unlock(blockId)
        }
      } else {
        removeBlockInternal(blockId, tellMaster = false)
        logWarning(s"Putting block $blockId failed")
      }
      res
    } catch {
      // Since removeBlockInternal may throw exception,
      // we should print exception first to show root cause.
      case NonFatal(e) =>
        logWarning(s"Putting block $blockId failed due to exception $e.")
        throw e
    } finally {
      // This cleanup is performed in a finally block rather than a `catch` to avoid having to
      // catch and properly re-throw InterruptedException.
      if (exceptionWasThrown) {
        // If an exception was thrown then it's possible that the code in `putBody` has already
        // notified the master about the availability of this block, so we need to send an update
        // to remove this block location.
        removeBlockInternal(blockId, tellMaster = tellMaster)
        // The `putBody` code may have also added a new block status to TaskMetrics, so we need
        // to cancel that out by overwriting it with an empty block status. We only do this if
        // the finally block was entered via an exception because doing this unconditionally would
        // cause us to send empty block statuses for every block that failed to be cached due to
        // a memory shortage (which is an expected failure, unlike an uncaught exception).
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
      }
    }
    val usedTimeMs = Utils.getUsedTimeNs(startTimeNs)
    if (level.replication > 1) {
      logDebug(s"Putting block ${blockId} with replication took $usedTimeMs")
    } else {
      logDebug(s"Putting block ${blockId} without replication took ${usedTimeMs}")
    }
    result
  }

  /**
   * Put the given block according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * If the block already exists, this method will not overwrite it.
   *
   * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
   *                     block already exists). If false, this method will hold no locks when it
   *                     returns.
   * @return None if the block was already present or if the put succeeded, or Some(iterator)
   *         if the put failed.
   */
  private def doPutIterator[T](
      blockId: BlockId,
      iterator: () => Iterator[T],
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Option[PartiallyUnrolledIterator[T]] = {
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      val startTimeNs = System.nanoTime()
      var iteratorFromFailedMemoryStorePut: Option[PartiallyUnrolledIterator[T]] = None
      // Size of the block in bytes
      var size = 0L
      if (level.useMemory) {
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        if (level.deserialized) {
          memoryStore.putIteratorAsValues(blockId, iterator(), classTag) match {
            case Right(s) =>
              size = s
            case Left(iter) =>
              // Not enough space to unroll this block; drop to disk if applicable
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  serializerManager.dataSerializeStream(blockId, out, iter)(classTag)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(iter)
              }
          }
        } else { // !level.deserialized
          memoryStore.putIteratorAsBytes(blockId, iterator(), classTag, level.memoryMode) match {
            case Right(s) =>
              size = s
            case Left(partiallySerializedValues) =>
              // Not enough space to unroll this block; drop to disk if applicable
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  partiallySerializedValues.finishWritingToStream(out)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(partiallySerializedValues.valuesIterator)
              }
          }
        }

      } else if (level.useDisk) {
        diskStore.put(blockId) { channel =>
          val out = Channels.newOutputStream(channel)
          serializerManager.dataSerializeStream(blockId, out, iterator())(classTag)
        }
        size = diskStore.getSize(blockId)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        // Now that the block is in either the memory or disk store, tell the master about it.
        info.size = size
        if (tellMaster && info.tellMaster) {
          reportBlockStatus(blockId, putBlockStatus)
        }
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        logDebug(s"Put block $blockId locally took ${Utils.getUsedTimeNs(startTimeNs)}")
        if (level.replication > 1) {
          val remoteStartTimeNs = System.nanoTime()
          val bytesToReplicate = doGetLocalBytes(blockId, info)
          // [SPARK-16550] Erase the typed classTag when using default serialization, since
          // NettyBlockRpcServer crashes when deserializing repl-defined classes.
          // TODO(ekl) remove this once the classloader issue on the remote end is fixed.
          val remoteClassTag = if (!serializerManager.canUseKryo(classTag)) {
            scala.reflect.classTag[Any]
          } else {
            classTag
          }
          try {
            replicate(blockId, bytesToReplicate, level, remoteClassTag)
          } finally {
            bytesToReplicate.dispose()
          }
          logDebug(s"Put block $blockId remotely took ${Utils.getUsedTimeNs(remoteStartTimeNs)}")
        }
      }
      assert(blockWasSuccessfullyStored == iteratorFromFailedMemoryStorePut.isEmpty)
      iteratorFromFailedMemoryStorePut
    }
  }

  /**
   * Attempts to cache spilled bytes read from disk into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the bytes from the memory store if the put succeeded, otherwise None.
   *         If this returns bytes from the memory store then the original disk store bytes will
   *         automatically be disposed and the caller should not continue to use them. Otherwise,
   *         if this returns None then the original disk store bytes will be unaffected.
   */
  private def maybeCacheDiskBytesInMemory(
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskData: BlockData): Option[ChunkedByteBuffer] = {
    require(!level.deserialized)
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          diskData.dispose()
          Some(memoryStore.getBytes(blockId).get)
        } else {
          val allocator = level.memoryMode match {
            case MemoryMode.ON_HEAP => ByteBuffer.allocate _
            case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
            case MemoryMode.PMEM => PersistentMemoryPlatform.allocateVolatileDirectBuffer _
          }
          val putSucceeded = memoryStore.putBytes(blockId, diskData.size, level.memoryMode, () => {
            // https://issues.apache.org/jira/browse/SPARK-6076
            // If the file size is bigger than the free memory, OOM will happen. So if we
            // cannot put it into MemoryStore, copyForMemory should not be created. That's why
            // this action is put into a `() => ChunkedByteBuffer` and created lazily.
            diskData.toChunkedByteBuffer(allocator)
          })
          if (putSucceeded) {
            diskData.dispose()
            Some(memoryStore.getBytes(blockId).get)
          } else {
            None
          }
        }
      }
    } else {
      None
    }
  }

  /**
   * Attempts to cache spilled values read from disk into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the iterator. The original iterator passed this method should no longer
   *         be used after this method returns.
   */
  private def maybeCacheDiskValuesInMemory[T](
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskIterator: Iterator[T]): Iterator[T] = {
    require(level.deserialized)
    val classTag = blockInfo.classTag.asInstanceOf[ClassTag[T]]
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          // Note: if we had a means to discard the disk iterator, we would do that here.
          memoryStore.getValues(blockId).get
        } else {
          memoryStore.putIteratorAsValues(blockId, diskIterator, classTag) match {
            case Left(iter) =>
              // The memory store put() failed, so it returned the iterator back to us:
              iter
            case Right(_) =>
              // The put() succeeded, so we can read the values back:
              memoryStore.getValues(blockId).get
          }
        }
      }.asInstanceOf[Iterator[T]]
    } else {
      diskIterator
    }
  }

  /**
   * Get peer block managers in the system.
   */
  private def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      val cachedPeersTtl = conf.get(config.STORAGE_CACHED_PEERS_TTL) // milliseconds
      val diff = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastPeerFetchTimeNs)
      val timeout = diff > cachedPeersTtl
      if (cachedPeers == null || forceFetch || timeout) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTimeNs = System.nanoTime()
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      cachedPeers
    }
  }

  /**
   * Called for pro-active replenishment of blocks lost due to executor failures
   *
   * @param blockId blockId being replicate
   * @param existingReplicas existing block managers that have a replica
   * @param maxReplicas maximum replicas needed
   */
  def replicateBlock(
      blockId: BlockId,
      existingReplicas: Set[BlockManagerId],
      maxReplicas: Int): Unit = {
    logInfo(s"Using $blockManagerId to pro-actively replicate $blockId")
    blockInfoManager.lockForReading(blockId).foreach { info =>
      val data = doGetLocalBytes(blockId, info)
      val storageLevel = StorageLevel(
        useDisk = info.level.useDisk,
        useMemory = info.level.useMemory,
        useOffHeap = info.level.useOffHeap,
        deserialized = info.level.deserialized,
        replication = maxReplicas)
      // we know we are called as a result of an executor removal, so we refresh peer cache
      // this way, we won't try to replicate to a missing executor with a stale reference
      getPeers(forceFetch = true)
      try {
        replicate(blockId, data, storageLevel, info.classTag, existingReplicas)
      } finally {
        logDebug(s"Releasing lock for $blockId")
        releaseLockAndDispose(blockId, data)
      }
    }
  }

  /**
   * Replicate block to another node. Note that this is a blocking call that returns after
   * the block has been replicated.
   */
  private def replicate(
      blockId: BlockId,
      data: BlockData,
      level: StorageLevel,
      classTag: ClassTag[_],
      existingReplicas: Set[BlockManagerId] = Set.empty): Unit = {

    val maxReplicationFailures = conf.get(config.STORAGE_MAX_REPLICATION_FAILURE)
    val tLevel = StorageLevel(
      useDisk = level.useDisk,
      useMemory = level.useMemory,
      useOffHeap = level.useOffHeap,
      deserialized = level.deserialized,
      replication = 1)

    val numPeersToReplicateTo = level.replication - 1
    val startTime = System.nanoTime

    val peersReplicatedTo = mutable.HashSet.empty ++ existingReplicas
    val peersFailedToReplicateTo = mutable.HashSet.empty[BlockManagerId]
    var numFailures = 0

    val initialPeers = getPeers(false).filterNot(existingReplicas.contains)

    var peersForReplication = blockReplicationPolicy.prioritize(
      blockManagerId,
      initialPeers,
      peersReplicatedTo,
      blockId,
      numPeersToReplicateTo)

    while(numFailures <= maxReplicationFailures &&
      !peersForReplication.isEmpty &&
      peersReplicatedTo.size < numPeersToReplicateTo) {
      val peer = peersForReplication.head
      try {
        val onePeerStartTime = System.nanoTime
        logTrace(s"Trying to replicate $blockId of ${data.size} bytes to $peer")
        // This thread keeps a lock on the block, so we do not want the netty thread to unlock
        // block when it finishes sending the message.
        val buffer = new BlockManagerManagedBuffer(blockInfoManager, blockId, data, false,
          unlockOnDeallocate = false)
        blockTransferService.uploadBlockSync(
          peer.host,
          peer.port,
          peer.executorId,
          blockId,
          buffer,
          tLevel,
          classTag)
        logTrace(s"Replicated $blockId of ${data.size} bytes to $peer" +
          s" in ${(System.nanoTime - onePeerStartTime).toDouble / 1e6} ms")
        peersForReplication = peersForReplication.tail
        peersReplicatedTo += peer
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to replicate $blockId to $peer, failure #$numFailures", e)
          peersFailedToReplicateTo += peer
          // we have a failed replication, so we get the list of peers again
          // we don't want peers we have already replicated to and the ones that
          // have failed previously
          val filteredPeers = getPeers(true).filter { p =>
            !peersFailedToReplicateTo.contains(p) && !peersReplicatedTo.contains(p)
          }

          numFailures += 1
          peersForReplication = blockReplicationPolicy.prioritize(
            blockManagerId,
            filteredPeers,
            peersReplicatedTo,
            blockId,
            numPeersToReplicateTo - peersReplicatedTo.size)
      }
    }
    logDebug(s"Replicating $blockId of ${data.size} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took ${(System.nanoTime - startTime) / 1e6} ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
    }

    logDebug(s"block $blockId replicated to ${peersReplicatedTo.mkString(", ")}")
  }

  /**
   * Read a block consisting of a single object.
   */
  def getSingle[T: ClassTag](blockId: BlockId): Option[T] = {
    get[T](blockId).map(_.data.next().asInstanceOf[T])
  }

  /**
   * Write a block consisting of a single object.
   *
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
   */
  def putSingle[T: ClassTag](
      blockId: BlockId,
      value: T,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   *
   * @return the block's new effective StorageLevel.
   */
  private[storage] override def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {
    logInfo(s"Dropping block $blockId from memory")
    val info = blockInfoManager.assertBlockIsLockedForWriting(blockId)
    var blockIsUpdated = false
    val level = info.level

    if (memExtensionEnabled) {
      val nvmStore = new NvmStore(conf, diskBlockManager, securityManager, executorId)
      // Drop to pmem, if storage level requires
      if (level.useDisk && !nvmStore.contains(blockId)) {
        logInfo(s"Writing block $blockId to pmem")
        data() match {
          case Left(elements) =>
            nvmStore.put(blockId) { channel =>
              val out = Channels.newOutputStream(channel)
              serializerManager.dataSerializeStream(
                blockId,
                out,
                elements.toIterator)(info.classTag.asInstanceOf[ClassTag[T]])
            }
          case Right(bytes) =>
            nvmStore.putBytes(blockId, bytes)
        }
        blockIsUpdated = true
      }
    } else {
      // Drop to disk, if storage level requires
      if (level.useDisk && !diskStore.contains(blockId)) {
        logInfo(s"Writing block $blockId to disk")
        data() match {
          case Left(elements) =>
            diskStore.put(blockId) { channel =>
              val out = Channels.newOutputStream(channel)
              serializerManager.dataSerializeStream(
                blockId,
                out,
                elements.toIterator)(info.classTag.asInstanceOf[ClassTag[T]])
            }
          case Right(bytes) =>
            diskStore.putBytes(blockId, bytes)
        }
        blockIsUpdated = true
      }
    }

    // Actually drop from memory store
    val droppedMemorySize =
      if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
    val blockIsRemoved = memoryStore.remove(blockId)
    if (blockIsRemoved) {
      blockIsUpdated = true
    } else {
      logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
    }

    val status = getCurrentBlockStatus(blockId, info)
    if (info.tellMaster) {
      reportBlockStatus(blockId, status, droppedMemorySize)
    }
    if (blockIsUpdated) {
      addUpdatedBlockStatusToTaskMetrics(blockId, status)
    }
    status.storageLevel
  }

  /**
   * Remove all blocks belonging to the given RDD.
   *
   * @return The number of blocks removed.
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfoManager.entries.flatMap(_._1.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  /**
   * Remove all blocks belonging to the given broadcast.
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfoManager.entries.map(_._1).collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
   * Remove a block from both memory and disk.
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"Removing block $blockId")
    blockInfoManager.lockForWriting(blockId) match {
      case None =>
        // The block has already been removed; do nothing.
        logWarning(s"Asked to remove block $blockId, which does not exist")
      case Some(info) =>
        removeBlockInternal(blockId, tellMaster = tellMaster && info.tellMaster)
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
    }
  }

  /**
   * Internal version of [[removeBlock()]] which assumes that the caller already holds a write
   * lock on the block.
   */
  private def removeBlockInternal(blockId: BlockId, tellMaster: Boolean): Unit = {
    val blockStatus = if (tellMaster) {
      val blockInfo = blockInfoManager.assertBlockIsLockedForWriting(blockId)
      Some(getCurrentBlockStatus(blockId, blockInfo))
    } else None

    // Removals are idempotent in disk store and memory store. At worst, we get a warning.
    val removedFromMemory = memoryStore.remove(blockId)
    val removedFromDisk = diskStore.remove(blockId)
    if (!removedFromMemory && !removedFromDisk) {
      logWarning(s"Block $blockId could not be removed as it was not found on disk or in memory")
    }

    blockInfoManager.removeBlock(blockId)
    if (tellMaster) {
      // Only update storage level from the captured block status before deleting, so that
      // memory size and disk size are being kept for calculating delta.
      reportBlockStatus(blockId, blockStatus.get.copy(storageLevel = StorageLevel.NONE))
    }
  }

  private def addUpdatedBlockStatusToTaskMetrics(blockId: BlockId, status: BlockStatus): Unit = {
    if (conf.get(config.TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES)) {
      Option(TaskContext.get()).foreach { c =>
        c.taskMetrics().incUpdatedBlockStatuses(blockId -> status)
      }
    }
  }

  def releaseLockAndDispose(
      blockId: BlockId,
      data: BlockData,
      taskContext: Option[TaskContext] = None): Unit = {
    releaseLock(blockId, taskContext)
    data.dispose()
  }

  def stop(): Unit = {
    blockTransferService.close()
    if (blockStoreClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      blockStoreClient.close()
    }
    remoteBlockTempFileManager.stop()
    diskBlockManager.stop()
    rpcEnv.stop(slaveEndpoint)
    blockInfoManager.clear()
    memoryStore.clear()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
  }
}


private[spark] object BlockManager {
  private val ID_GENERATOR = new IdGenerator

  def blockIdsToLocations(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map { loc =>
        ExecutorCacheTaskLocation(loc.host, loc.executorId).toString
      }
    }
    blockManagers.toMap
  }

  private class ShuffleMetricsSource(
      override val sourceName: String,
      metricSet: MetricSet) extends Source {

    override val metricRegistry = new MetricRegistry
    metricRegistry.registerAll(metricSet)
  }

  class RemoteBlockDownloadFileManager(blockManager: BlockManager)
      extends DownloadFileManager with Logging {
    // lazy because SparkEnv is set after this
    lazy val encryptionKey = SparkEnv.get.securityManager.getIOEncryptionKey()

    private class ReferenceWithCleanup(
        file: DownloadFile,
        referenceQueue: JReferenceQueue[DownloadFile]
        ) extends WeakReference[DownloadFile](file, referenceQueue) {

      val filePath = file.path()

      def cleanUp(): Unit = {
        logDebug(s"Clean up file $filePath")

        if (!file.delete()) {
          logDebug(s"Fail to delete file $filePath")
        }
      }
    }

    private val referenceQueue = new JReferenceQueue[DownloadFile]
    private val referenceBuffer = Collections.newSetFromMap[ReferenceWithCleanup](
      new ConcurrentHashMap)

    private val POLL_TIMEOUT = 1000
    @volatile private var stopped = false

    private val cleaningThread = new Thread() { override def run(): Unit = { keepCleaning() } }
    cleaningThread.setDaemon(true)
    cleaningThread.setName("RemoteBlock-temp-file-clean-thread")
    cleaningThread.start()

    override def createTempFile(transportConf: TransportConf): DownloadFile = {
      val file = blockManager.diskBlockManager.createTempLocalBlock()._2
      encryptionKey match {
        case Some(key) =>
          // encryption is enabled, so when we read the decrypted data off the network, we need to
          // encrypt it when writing to disk.  Note that the data may have been encrypted when it
          // was cached on disk on the remote side, but it was already decrypted by now (see
          // EncryptedBlockData).
          new EncryptedDownloadFile(file, key)
        case None =>
          new SimpleDownloadFile(file, transportConf)
      }
    }

    override def registerTempFileToClean(file: DownloadFile): Boolean = {
      referenceBuffer.add(new ReferenceWithCleanup(file, referenceQueue))
    }

    def stop(): Unit = {
      stopped = true
      cleaningThread.interrupt()
      cleaningThread.join()
    }

    private def keepCleaning(): Unit = {
      while (!stopped) {
        try {
          Option(referenceQueue.remove(POLL_TIMEOUT))
            .map(_.asInstanceOf[ReferenceWithCleanup])
            .foreach { ref =>
              referenceBuffer.remove(ref)
              ref.cleanUp()
            }
        } catch {
          case _: InterruptedException =>
            // no-op
          case NonFatal(e) =>
            logError("Error in cleaning thread", e)
        }
      }
    }
  }

  /**
   * A DownloadFile that encrypts data when it is written, and decrypts when it's read.
   */
  private class EncryptedDownloadFile(
      file: File,
      key: Array[Byte]) extends DownloadFile {

    private val env = SparkEnv.get

    override def delete(): Boolean = file.delete()

    override def openForWriting(): DownloadFileWritableChannel = {
      new EncryptedDownloadWritableChannel()
    }

    override def path(): String = file.getAbsolutePath

    private class EncryptedDownloadWritableChannel extends DownloadFileWritableChannel {
      private val countingOutput: CountingWritableChannel = new CountingWritableChannel(
        Channels.newChannel(env.serializerManager.wrapForEncryption(new FileOutputStream(file))))

      override def closeAndRead(): ManagedBuffer = {
        countingOutput.close()
        val size = countingOutput.getCount
        new EncryptedManagedBuffer(new EncryptedBlockData(file, size, env.conf, key))
      }

      override def write(src: ByteBuffer): Int = countingOutput.write(src)

      override def isOpen: Boolean = countingOutput.isOpen()

      override def close(): Unit = countingOutput.close()
    }
  }
}
