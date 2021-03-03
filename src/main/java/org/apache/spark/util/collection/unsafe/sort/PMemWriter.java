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

import com.intel.oap.common.unsafe.PersistentMemoryPlatform;

import org.apache.spark.SparkEnv;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * In this writer, records page along with LongArray page are both dumped to PMem when spill happens
 */
public final class PMemWriter extends UnsafeSorterPMemSpillWriter {
    private static final Logger logger = LoggerFactory.getLogger(PMemWriter.class);
    private LongArray sortedArray;
    private HashMap<MemoryBlock, MemoryBlock> pageMap = new HashMap<>();
    private int position;
    private LinkedList<MemoryBlock> allocatedDramPages;
    private MemoryBlock pMemPageForLongArray;
    private UnsafeSorterSpillWriter diskSpillWriter;
    private BlockManager blockManager;
    private SerializerManager serializerManager;
    private int fileBufferSize;
    private boolean isSorted;
    private int totalRecordsWritten;
    private final boolean spillToPMemConcurrently = SparkEnv.get() != null && (boolean) SparkEnv.get().conf().get(
            package$.MODULE$.MEMORY_SPILL_PMEM_SORT_BACKGROUND());
    private final boolean pMemClflushEnabled = SparkEnv.get() != null &&
        (boolean)SparkEnv.get().conf().get(package$.MODULE$.MEMORY_SPILL_PMEM_CLFLUSH_ENABLED());

    public PMemWriter(
            UnsafeExternalSorter externalSorter,
            SortedIteratorForSpills sortedIterator,
            boolean isSorted,
            int numberOfRecordsToWritten,
            SerializerManager serializerManager,
            BlockManager blockManager,
            int fileBufferSize,
            ShuffleWriteMetrics writeMetrics,
            TaskMetrics taskMetrics) {
        // SortedIterator is null or readingIterator from UnsafeExternalSorter.
        // But it isn't used in this PMemWriter, only for keep same constructor with other spill writers.
        super(externalSorter, sortedIterator, numberOfRecordsToWritten, writeMetrics, taskMetrics);
        this.allocatedDramPages = externalSorter.getAllocatedPages();
        this.blockManager = blockManager;
        this.serializerManager = serializerManager;
        this.fileBufferSize = fileBufferSize;
        this.isSorted = isSorted;
        // In the case that spill happens when iterator isn't sorted yet, the valid records
        // will be [0, inMemsorter.numRecords]. When iterator is sorted, the valid records will be
        // [position/2, inMemsorter.numRecords]
        this.totalRecordsWritten = externalSorter.getInMemSorter().numRecords();
    }

    @Override
    public void write() throws IOException {
        // write records based on externalsorter
        // try to allocate all needed PMem pages before spill to PMem
        UnsafeInMemorySorter inMemSorter = externalSorter.getInMemSorter();
        if (allocatePMemPages(allocatedDramPages, inMemSorter.getArray().memoryBlock())) {
            if (spillToPMemConcurrently && !isSorted) {
                logger.info("Concurrent PMem write/records sort");
                long writeDuration = 0;
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                Future<Long> future = executorService.submit(()->dumpPagesToPMem());
                inMemSorter.getSortedIterator();
                try {
                    writeDuration = future.get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.error(e.getMessage());
                }
                executorService.shutdownNow();
                updateLongArray(inMemSorter.getArray(), totalRecordsWritten, 0);
            } else if(!isSorted) {
                dumpPagesToPMem();
                // get sorted iterator
                inMemSorter.getSortedIterator();
                // update LongArray
                updateLongArray(inMemSorter.getArray(), totalRecordsWritten, 0);
            } else {
                dumpPagesToPMem();
                // get sorted iterator
                assert(sortedIterator != null);
                updateLongArray(inMemSorter.getArray(), totalRecordsWritten, sortedIterator.getPosition());
            }
        } else {
            // fallback to disk spill
            if (diskSpillWriter == null) {
                diskSpillWriter = new UnsafeSorterSpillWriter(
                        blockManager,
                        fileBufferSize,
                        isSorted? sortedIterator : inMemSorter.getSortedIterator(),
                        numberOfRecordsToWritten,
                        serializerManager,
                        writeMetrics,
                        taskMetrics);
            }
            diskSpillWriter.write(false);
        }
    }

    public boolean allocatePMemPages(LinkedList<MemoryBlock> dramPages, MemoryBlock longArrayPage) {
        for (MemoryBlock page: dramPages) {
            MemoryBlock pMemBlock = taskMemoryManager.allocatePMemPage(page.size());
            if (pMemBlock != null) {
                allocatedPMemPages.add(pMemBlock);
                pageMap.put(page, pMemBlock);
            } else {
                freeAllPMemPages();
                pageMap.clear();
                return false;
            }
        }
        pMemPageForLongArray = taskMemoryManager.allocatePMemPage(longArrayPage.size());
        if (pMemPageForLongArray != null) {
            allocatedPMemPages.add(pMemPageForLongArray);
            pageMap.put(longArrayPage, pMemPageForLongArray);
        } else {
            freeAllPMemPages();
            pageMap.clear();
            return false;
        }
        return (allocatedPMemPages.size() == dramPages.size() + 1);
    }

    private long dumpPagesToPMem() {
        long dumpTime = System.nanoTime();
        for (MemoryBlock page : allocatedDramPages) {
            dumpPageToPMem(page);
        }
        long dumpDuration = System.nanoTime() - dumpTime;
        return dumpDuration;

    }

    private void dumpPageToPMem(MemoryBlock page) {
        MemoryBlock pMemBlock = pageMap.get(page);
        PersistentMemoryPlatform.copyMemory(
            page.getBaseObject(), page.getBaseOffset(),
            null, pMemBlock.getBaseOffset(), page.size(),
            pMemClflushEnabled);
        writeMetrics.incBytesWritten(page.size());
    }

    public void updateLongArray(LongArray sortedArray, int numRecords, int position) {
        this.position = position;
        while (position < numRecords * 2){
            // update recordPointer in this array
            long originalRecordPointer = sortedArray.get(position);
            MemoryBlock page = taskMemoryManager.getOriginalPage(originalRecordPointer);
            long offset = taskMemoryManager.getOffsetInPage(originalRecordPointer) - page.getBaseOffset();
            MemoryBlock pMemBlock = pageMap.get(page);
            long pMemOffset = pMemBlock.getBaseOffset() + offset;
            sortedArray.set(position, pMemOffset);
            position += 2;
        }
        // copy the LongArray to PMem
        MemoryBlock arrayBlock = sortedArray.memoryBlock();
        MemoryBlock pMemBlock = pageMap.get(arrayBlock);
        PersistentMemoryPlatform.copyMemory(
            arrayBlock.getBaseObject(), arrayBlock.getBaseOffset(),
            null, pMemBlock.getBaseOffset(), arrayBlock.size(),
            pMemClflushEnabled);
        writeMetrics.incBytesWritten(pMemBlock.size());
        this.sortedArray = new LongArray(pMemBlock);
    }

    @Override
    public UnsafeSorterIterator getSpillReader() throws IOException {
        // TODO: consider partial spill to PMem + Disk.
        if (diskSpillWriter != null) {
            return diskSpillWriter.getSpillReader();
        } else {
            return new PMemReaderForUnsafeExternalSorter(sortedArray, position, totalRecordsWritten, taskMetrics);
        }
    }

    public void clearAll() {
        freeAllPMemPages();
        if (diskSpillWriter != null) {
            diskSpillWriter.clearAll();
        }
    }

    @Override
    public int recordsSpilled() {
        return numberOfRecordsToWritten;
    }

    @Override
    public void freeAllPMemPages() {
        for( MemoryBlock page: allocatedPMemPages) {
            taskMemoryManager.freePMemPage(page, externalSorter);
        }
        allocatedPMemPages.clear();
    }
}
