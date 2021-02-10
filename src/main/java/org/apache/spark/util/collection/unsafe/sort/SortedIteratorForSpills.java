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

package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;

public class SortedIteratorForSpills extends UnsafeSorterIterator {
    private LongArray sortedArray;
    private final int numRecords;
    private int position;
    private int offset;
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;
    private int recordLength;
    private long currentPageNumber;
    private final TaskContext taskContext = TaskContext.get();
    private final TaskMemoryManager memoryManager;

    /**
     * Construct an iterator to read the spill.
     * @param array the array here should be already sorted.
     * @param numRecords the number of the records recorded in this LongArray
     * @param offset
     */
    public SortedIteratorForSpills(
      final TaskMemoryManager memoryManager,
      LongArray array,
      int numRecords,
      int offset) {
        this.memoryManager = memoryManager;
        this.sortedArray = array;
        this.numRecords = numRecords;
        this.position = 0;
        this.offset = offset;
    }

    public static SortedIteratorForSpills createFromExistingSorterIte(
            UnsafeInMemorySorter.SortedIterator sortedIte,
            UnsafeInMemorySorter inMemSorter) {
        if (sortedIte == null) {
            return null;
        }
        TaskMemoryManager taskMemoryManager = inMemSorter.getTaskMemoryManager();
        LongArray array = inMemSorter.getLongArray();
        int numberRecords = sortedIte.getNumRecords();
        int offset = sortedIte.getOffset();
        int position = sortedIte.getPosition();
        SortedIteratorForSpills spillIte = new SortedIteratorForSpills(taskMemoryManager, array,numberRecords,offset);
        spillIte.pointTo(position);
        return spillIte;
    }

    @Override
    public int getNumRecords() {
        return numRecords;
    }

    @Override
    public boolean hasNext() {
        return position / 2 < numRecords;
    }

    @Override
    public void loadNext() {
        // Kill the task in case it has been marked as killed. This logic is from
        // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
        // to avoid performance overhead. This check is added here in `loadNext()` instead of in
        // `hasNext()` because it's technically possible for the caller to be relying on
        // `getNumRecords()` instead of `hasNext()` to know when to stop.
        if (taskContext != null) {
            taskContext.killTaskIfInterrupted();
        }
        loadPosition();
    }
    /**
     * load the record of current position and move to end of the record.
     */
    private void loadPosition() {
        // This pointer points to a 4-byte record length, followed by the record's bytes
        final long recordPointer = sortedArray.get(offset + position);
        currentPageNumber = TaskMemoryManager.decodePageNumber(recordPointer);
        int uaoSize = UnsafeAlignedOffset.getUaoSize();
        baseObject = memoryManager.getPage(recordPointer);
        // Skip over record length
        baseOffset = memoryManager.getOffsetInPage(recordPointer) + uaoSize;
        recordLength = UnsafeAlignedOffset.getSize(baseObject, baseOffset - uaoSize);
        keyPrefix = sortedArray.get(offset + position + 1);
        position += 2;
    }

    /**
     * point to a given position.
     * @param pos
     */
    public void pointTo(int pos) {
        if (pos % 2 != 0) {
            throw new IllegalArgumentException("Can't point to the middle of a record.");
        }
        position = pos;
    }

    @Override
    public Object getBaseObject() { return baseObject; }

    @Override
    public long getBaseOffset() { return baseOffset; }

    public long getCurrentPageNumber() { return currentPageNumber; }

    @Override
    public int getRecordLength() { return recordLength; }

    @Override
    public long getKeyPrefix() { return keyPrefix; }

    public LongArray getLongArray() { return sortedArray; }

    public int getPosition() { return position; }
}
