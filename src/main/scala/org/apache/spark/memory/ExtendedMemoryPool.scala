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
package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.internal.Logging

private[memory] class ExtendedMemoryPool(lock: Object) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = "extended memory"

  /**
   * Map from taskAttemptId -> memory consumption in bytes
   */
  @GuardedBy("lock")
  private val extendedMemoryForTask = new mutable.HashMap[Long, Long]()

  @GuardedBy("lock")
  private[this] var _memoryUsed: Long = 0L

  override def memoryUsed: Long = lock.synchronized {
    _memoryUsed
  }
  /**
   * Returns the memory consumption, in bytes, for the given task.
   */
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    extendedMemoryForTask.getOrElse(taskAttemptId, 0L)
  }

  /**
   * Try to acquire up to `numBytes` of extended memory for the given task and return the number
   * of bytes obtained, or 0 if none can be allocated.
   *
   * @param numBytes           number of bytes to acquire
   * @param taskAttemptId      the task attempt acquiring memory
   * @return the number of bytes granted to the task.
   */
  private[memory] def acquireMemory(
      numBytes: Long,
      taskAttemptId: Long): Long = lock.synchronized {
    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
    if (!extendedMemoryForTask.contains(taskAttemptId)) {
      extendedMemoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      lock.notifyAll()
    }

    if (memoryFree >= numBytes) {
      _memoryUsed += numBytes;
      extendedMemoryForTask(taskAttemptId) += numBytes
      return numBytes
    }
    0L // Never reached
  }

  /**
   * Release `numBytes` of extended memory acquired by the given task.
   */
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val curMem = extendedMemoryForTask.getOrElse(taskAttemptId, 0L)
    val memoryToFree = if (curMem < numBytes) {
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      curMem
    } else {
      numBytes
    }
    if (extendedMemoryForTask.contains(taskAttemptId)) {
      extendedMemoryForTask(taskAttemptId) -= memoryToFree
      if (extendedMemoryForTask(taskAttemptId) <= 0) {
        extendedMemoryForTask.remove(taskAttemptId)
      }
    }
    _memoryUsed -= memoryToFree
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   *
   * @return the number of bytes freed.
   */
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val numBytesToFree = getMemoryUsageForTask(taskAttemptId)
    releaseMemory(numBytesToFree, taskAttemptId)
    numBytesToFree
  }
}

