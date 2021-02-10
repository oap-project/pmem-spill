package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TempLocalBlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;

public class UnsafeSorterStreamSpillWriter extends UnsafeSorterSpillWriter {
  private static final Logger logger = LoggerFactory.getLogger(UnsafeSorterStreamSpillWriter.class);
  private UnsafeSorterStreamSpillReader reader;
  public UnsafeSorterStreamSpillWriter(
      BlockManager blockManager,
      int fileBufferSize,
      UnsafeSorterIterator inMemIterator,
      int numRecordsToWrite,
      SerializerManager serializerManager,
      ShuffleWriteMetrics writeMetrics,
      TaskMetrics taskMetrics) {
    super();
    final Tuple2<TempLocalBlockId, File> spilledFileInfo =
            blockManager.diskBlockManager().createTempLocalBlock();
    this.file = spilledFileInfo._2();
    this.blockId = spilledFileInfo._1();
    this.numRecordsToWrite = numRecordsToWrite;
    this.serializerManager = serializerManager;
    this.taskMetrics = taskMetrics;
    this.inMemIterator = inMemIterator;
    // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.
    writer = blockManager.getPMemWriter(
            blockId, file, DummySerializerInstance.INSTANCE, fileBufferSize, writeMetrics);
    // Write the number of records
    writeIntToBuffer(numRecordsToWrite, 0);
    writer.write(writeBuffer, 0, 4);
  }

  @Override
  public UnsafeSorterIterator getSpillReader() throws IOException {
    reader = new UnsafeSorterStreamSpillReader(serializerManager, taskMetrics, file, blockId);
    return reader;
  }

  @Override
  public void clearAll() {
    assert(reader != null);
    try {
        reader.chunkInputStream.free();
    } catch (IOException e) {
        logger.debug(e.toString());
    }
  }
}
