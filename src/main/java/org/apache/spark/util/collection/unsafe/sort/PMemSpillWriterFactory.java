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
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockManager;

import java.io.IOException;

public class PMemSpillWriterFactory {
    public static SpillWriterForUnsafeSorter getSpillWriter(
            PMemSpillWriterType writerType,
            UnsafeExternalSorter externalSorter,
            UnsafeSorterIterator sortedIterator,
            int numberOfRecordsToWritten,
            SerializerManager serializerManager,
            BlockManager blockManager,
            int fileBufferSize,
            ShuffleWriteMetrics writeMetrics,
            TaskMetrics taskMetrics,
            boolean spillToPMEMEnabled,
            boolean isSorted) throws IOException {
        if (spillToPMEMEnabled && writerType == PMemSpillWriterType.MEM_COPY_ALL_DATA_PAGES_TO_PMEM_WITHLONGARRAY){
            SortedIteratorForSpills sortedSpillIte = SortedIteratorForSpills.createFromExistingSorterIte(
                    (UnsafeInMemorySorter.SortedIterator)sortedIterator,
                    externalSorter.getInMemSorter());
            return new PMemWriter(
                    externalSorter,
                    sortedSpillIte,
                    isSorted,
                    numberOfRecordsToWritten,
                    serializerManager,
                    blockManager,
                    fileBufferSize,
                    writeMetrics,
                    taskMetrics);
        } else {
            if (sortedIterator == null) {
                sortedIterator = externalSorter.getInMemSorter().getSortedIterator();
            }
            if (spillToPMEMEnabled && sortedIterator instanceof UnsafeInMemorySorter.SortedIterator){

                if (writerType == PMemSpillWriterType.WRITE_SORTED_RECORDS_TO_PMEM) {
                    SortedIteratorForSpills sortedSpillIte = SortedIteratorForSpills.createFromExistingSorterIte(
                            (UnsafeInMemorySorter.SortedIterator)sortedIterator,
                            externalSorter.getInMemSorter());
                    return new SortedPMemPageSpillWriter(
                            externalSorter,
                            sortedSpillIte,
                            numberOfRecordsToWritten,
                            serializerManager,
                            blockManager,
                            fileBufferSize,
                            writeMetrics,
                            taskMetrics);
                }
                if (spillToPMEMEnabled && writerType == PMemSpillWriterType.STREAM_SPILL_TO_PMEM) {
                    return new UnsafeSorterStreamSpillWriter(
                            blockManager,
                            fileBufferSize,
                            sortedIterator,
                            numberOfRecordsToWritten,
                            serializerManager,
                            writeMetrics,
                            taskMetrics);
                }
            } else {
                return new UnsafeSorterSpillWriter(
                        blockManager,
                        fileBufferSize,
                        sortedIterator,
                        numberOfRecordsToWritten,
                        serializerManager,
                        writeMetrics,
                        taskMetrics);
            }

        }
        return null;
    }
}
