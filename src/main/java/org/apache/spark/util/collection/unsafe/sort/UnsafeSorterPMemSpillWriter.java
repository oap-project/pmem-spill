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

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.unsafe.memory.MemoryBlock;

import java.io.IOException;
import java.util.LinkedList;

public abstract class UnsafeSorterPMemSpillWriter implements SpillWriterForUnsafeSorter{
    /**
     * the memConsumer used to allocate pmem pages
     */
    protected UnsafeExternalSorter externalSorter;

    protected SortedIteratorForSpills sortedIterator;

    protected int numberOfRecordsToWritten = 0;

    protected TaskMemoryManager taskMemoryManager;

    //Todo: It's confusing to have ShuffleWriteMetrics here. will reconsider and fix it later.
    protected ShuffleWriteMetrics writeMetrics;

    protected TaskMetrics taskMetrics;

    protected LinkedList<MemoryBlock> allocatedPMemPages = new LinkedList<MemoryBlock>();

    //Page size in bytes.
    private static long DEFAULT_PAGE_SIZE = 64*1024*1024;

    public UnsafeSorterPMemSpillWriter(
        UnsafeExternalSorter externalSorter,
        SortedIteratorForSpills sortedIterator,
        int numberOfRecordsToWritten,
        ShuffleWriteMetrics writeMetrics,
        TaskMetrics taskMetrics) {
        this.externalSorter = externalSorter;
        this.taskMemoryManager = externalSorter.getTaskMemoryManager();
        this.sortedIterator = sortedIterator;
        this.numberOfRecordsToWritten = numberOfRecordsToWritten;
        this.writeMetrics = writeMetrics;
        this.taskMetrics = taskMetrics;
    }

    protected MemoryBlock allocatePMemPage() throws IOException { ;
        return allocatePMemPage(DEFAULT_PAGE_SIZE);
    }

    protected MemoryBlock allocatePMemPage(long size) {
        MemoryBlock page = taskMemoryManager.allocatePage(size, externalSorter, true);
        if (page != null) {
            allocatedPMemPages.add(page);
        }
        return page;
    }

    protected void freeAllPMemPages() {
        for (MemoryBlock page : allocatedPMemPages) {
            taskMemoryManager.freePage(page, externalSorter);
        }
        allocatedPMemPages.clear();
    }
    public abstract UnsafeSorterIterator getSpillReader() throws IOException;
}
