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
import org.apache.spark.internal.config.package$;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.serializer.SerializerManager;
import java.io.IOException;
import java.util.LinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortedPMemPageSpillWriter extends UnsafeSorterPMemSpillWriter {
    private static final Logger sorted_logger = LoggerFactory.getLogger(SortedPMemPageSpillWriter.class);
    private MemoryBlock currentPMemPage = null;
    private long currentOffsetInPage = 0L;
    private int currentNumOfRecordsInPage = 0;
    //Page -> record number map
    private LinkedHashMap<MemoryBlock,Integer> pageNumOfRecMap = new LinkedHashMap<MemoryBlock,Integer>();
    private int numRecords = 0;
    private int numRecordsOnPMem = 0;

    private BlockManager blockManager;
    private SerializerManager serializerManager;
    private int fileBufferSize = 0;
    private UnsafeSorterSpillWriter diskSpillWriter;

    private final boolean pMemClflushEnabled = SparkEnv.get() != null &&
        (boolean)SparkEnv.get().conf().get(package$.MODULE$.MEMORY_SPILL_PMEM_CLFLUSH_ENABLED());

    public SortedPMemPageSpillWriter(
            UnsafeExternalSorter externalSorter,
            SortedIteratorForSpills sortedIterator,
            int numberOfRecordsToWritten,
            SerializerManager serializerManager,
            BlockManager blockManager,
            int fileBufferSize,
            ShuffleWriteMetrics writeMetrics,
            TaskMetrics taskMetrics) {
        super(externalSorter, sortedIterator, numberOfRecordsToWritten, writeMetrics, taskMetrics);
        this.blockManager = blockManager;
        this.serializerManager = serializerManager;
        this.fileBufferSize = fileBufferSize;
    }

    @Override
    public void write() throws IOException {
        boolean allBeWritten = writeToPMem();
        if (!allBeWritten) {
            sorted_logger.debug("No more PMEM space available. Write left spills to disk.");
            writeToDisk();
        }
    }

    /**
     * @return if all records have been write to PMem, return true. Otherwise, return false.
     * @throws IOException
     */
    private boolean writeToPMem() throws IOException {
        while (sortedIterator.hasNext()) {
            sortedIterator.loadNext();
            final Object baseObject = sortedIterator.getBaseObject();
            final long baseOffset = sortedIterator.getBaseOffset();
            int curRecLen = sortedIterator.getRecordLength();
            long curPrefix = sortedIterator.getKeyPrefix();
            if (needNewPMemPage(curRecLen)) {
                currentPMemPage = allocatePMemPage();
            }
            if (currentPMemPage != null) {
                long pageBaseOffset = currentPMemPage.getBaseOffset();
                long curPMemOffset = pageBaseOffset + currentOffsetInPage;
                writeRecordToPMem(baseObject, baseOffset, curRecLen, curPrefix, curPMemOffset);
                currentNumOfRecordsInPage ++;
                pageNumOfRecMap.put(currentPMemPage, currentNumOfRecordsInPage);
                numRecords ++;
            } else {
                //No more PMem space available, current loaded record can't be written to PMem.
                return false;
            }
        }
        //All records have been written to PMem.
        return true;
    }

    private void writeToDisk() throws IOException{
        int numOfRecLeft = numberOfRecordsToWritten - numRecordsOnPMem;
        if (diskSpillWriter == null) {
            diskSpillWriter = new UnsafeSorterSpillWriter(
                    blockManager,
                    fileBufferSize,
                    sortedIterator,
                    numOfRecLeft,
                    serializerManager,
                    writeMetrics,
                    taskMetrics);
        }
        diskSpillWriter.write(true);
        sorted_logger.info("Num of rec {}; Num of rec written to PMem {}; still {} records left; num of rec written to disk {}.",
                sortedIterator.getNumRecords(),
                numRecordsOnPMem,
                numOfRecLeft,
                diskSpillWriter.recordsSpilled());
    }
    
    private boolean needNewPMemPage(int nextRecLen) {
        if (allocatedPMemPages.isEmpty()) {
            return true;
        }
        else {
            long pageBaseOffset = currentPMemPage.getBaseOffset();
            long leftLenInCurPage = currentPMemPage.size() - currentOffsetInPage;
            int uaoSize = UnsafeAlignedOffset.getUaoSize();
            long recSizeRequired = uaoSize + Long.BYTES + nextRecLen;
            if (leftLenInCurPage < recSizeRequired) {
                return true;
            }
        }
        return false;
    }

    private void writeRecordToPMem(Object baseObject, long baseOffset, int recLength, long prefix, long pMemOffset){
        Platform.putInt(
                null,
                pMemOffset,
                recLength);
        int uaoSize = UnsafeAlignedOffset.getUaoSize();
        long currentOffset = pMemOffset + uaoSize;
        Platform.putLong(
                null,
                currentOffset,
                prefix);
        currentOffset += Long.BYTES;
        PersistentMemoryPlatform.copyMemory(
                baseObject,
                baseOffset,
                null,
                currentOffset,
                recLength,
                pMemClflushEnabled);
        numRecordsOnPMem ++;
        currentOffsetInPage += uaoSize + Long.BYTES + recLength;
    }

    protected MemoryBlock allocatePMemPage() throws IOException{
        currentPMemPage = super.allocatePMemPage();
        currentOffsetInPage = 0;
        currentNumOfRecordsInPage = 0;
        return currentPMemPage;
    }

    @Override
    public UnsafeSorterIterator getSpillReader() throws IOException {
        return new SortedPMemPageSpillReader();
    }

    @Override
    public void clearAll() {
        freeAllPMemPages();
        if (diskSpillWriter != null) {
            diskSpillWriter.clearAll();
        }
    }

    public int recordsSpilled() {
        int recordsSpilledOnDisk = 0;
        if (diskSpillWriter != null) {
            recordsSpilledOnDisk = diskSpillWriter.recordsSpilled();
        }
        return numRecordsOnPMem + recordsSpilledOnDisk;
    }

    private class SortedPMemPageSpillReader extends UnsafeSorterIterator {
        private final Logger sorted_reader_logger = LoggerFactory.getLogger(SortedPMemPageSpillReader.class);
        private MemoryBlock curPage = null;
        private int curPageIdx = -1;
        private int curOffsetInPage = 0;
        private int curNumOfRecInPage = 0;
        private int curNumOfRec = 0;
        private Object baseObject = null;
        private long baseOffset = 0;
        private int recordLength;
        private long keyPrefix;
        private UnsafeSorterIterator diskSpillReader;
        private int numRecordsOnDisk = 0;

        public SortedPMemPageSpillReader() throws IOException{
            if (diskSpillWriter != null) {
                diskSpillReader = diskSpillWriter.getSpillReader();
                numRecordsOnDisk = diskSpillWriter.recordsSpilled();
            }
        }
        @Override
        public boolean hasNext() {
            return curNumOfRec < numRecordsOnPMem + numRecordsOnDisk;
        }
        @Override
        public void loadNext() throws IOException {
            if(curNumOfRec < numRecordsOnPMem) {
                loadNextOnPMem();
            } else {
                loadNextOnDisk();
            }
        }

        private void loadNextOnPMem() throws IOException {
            if (curPage == null || curNumOfRecInPage >= pageNumOfRecMap.get(curPage)) {
                moveToNextPMemPage();
            }
            long curPageBaseOffset = curPage.getBaseOffset();
            recordLength = UnsafeAlignedOffset.getSize(null, curPageBaseOffset + curOffsetInPage);
            curOffsetInPage += UnsafeAlignedOffset.getUaoSize();
            keyPrefix = Platform.getLong(null, curPageBaseOffset + curOffsetInPage);
            curOffsetInPage += Long.BYTES;
            baseOffset = curPageBaseOffset + curOffsetInPage;
            curOffsetInPage += recordLength;
            curNumOfRecInPage ++;
            curNumOfRec ++;
        }

        private void loadNextOnDisk() throws IOException {
            if (diskSpillReader != null && diskSpillReader.hasNext()) {
                diskSpillReader.loadNext();
                baseObject = diskSpillReader.getBaseObject();
                baseOffset = diskSpillReader.getBaseOffset();
                recordLength = diskSpillReader.getRecordLength();
                keyPrefix = diskSpillReader.getKeyPrefix();
                curNumOfRec ++;
            }
        }

        private void moveToNextPMemPage() {
            curPageIdx++;
            curPage = allocatedPMemPages.get(curPageIdx);
            curOffsetInPage = 0;
            curNumOfRecInPage = 0;
        }

        @Override
        public Object getBaseObject() {
            return baseObject;
        }

        @Override
        public long getBaseOffset() {
            return baseOffset;
        }

        @Override
        public int getRecordLength() {
            return recordLength;
        }

        @Override
        public long getKeyPrefix() {
            return keyPrefix;
        }

        @Override
        public int getNumRecords() {
            return numRecordsOnPMem + numRecordsOnDisk;
        }
    }
}
