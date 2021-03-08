package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.SparkEnv;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class PMemReaderForUnsafeExternalSorter extends UnsafeSorterIterator implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(PMemReaderForUnsafeExternalSorter.class);
    private int recordLength;
    private long keyPrefix;
    private int numRecordsRemaining;
    private int numRecords;
    private LongArray sortedArray;
    private int position;
    private byte[] arr = new byte[1024 * 1024];
    private byte[] bytes = new byte[1024 * 1024];
    private Object baseObject = arr;
    private TaskMetrics taskMetrics;
    private long startTime;
    private ByteBuffer byteBuffer;
    public PMemReaderForUnsafeExternalSorter(
            LongArray sortedArray, int position,  int numRecords, TaskMetrics taskMetrics) {
        this.sortedArray = sortedArray;
        this.position = position;
        this.numRecords = numRecords;
        this.numRecordsRemaining = numRecords - position/2;
        this.taskMetrics = taskMetrics;
        int readBufferSize = SparkEnv.get() == null? 8 * 1024 * 1024 :
                (int) (long) SparkEnv.get().conf().get(package$.MODULE$.MEMORY_SPILL_PMEM_READ_BUFFERSIZE());
        logger.info("PMem read buffer size is:" + Utils.bytesToString(readBufferSize));
        this.byteBuffer = ByteBuffer.wrap(new byte[readBufferSize]);
        byteBuffer.flip();
        byteBuffer.order(ByteOrder.nativeOrder());
    }

    @Override
    public void loadNext() {
        if (!byteBuffer.hasRemaining()) {
            boolean refilled = refill();
            if (!refilled) {
                logger.error("Illegal status: records finished read but hasNext() is true.");
            }
        }
        keyPrefix = byteBuffer.getLong();
        recordLength = byteBuffer.getInt();
        if (recordLength > arr.length) {
            arr = new byte[recordLength];
            baseObject = arr;
        }
        byteBuffer.get(arr, 0, recordLength);
        numRecordsRemaining --;
    }

    @Override
    public int getNumRecords() {
        return numRecords;
    }

    /**
     * load more PMem records in the buffer
     */
    private boolean refill() {
        byteBuffer.clear();
        int nRead = loadData();
        byteBuffer.flip();
        if (nRead <= 0) {
            return false;
        }
        return true;
    }

    private int loadData() {
        // no records remaining to read
        if (position >= numRecords * 2)
            return -1;
        int bufferPos = 0;
        int capacity = byteBuffer.capacity();
        while (bufferPos < capacity && position < numRecords * 2) {
            long curRecordAddress = sortedArray.get(position);
            int recordLen = Platform.getInt(null, curRecordAddress);
            // length + keyprefix + record length
            int length = Integer.BYTES + Long.BYTES + recordLen;
            if (length > capacity) {
                logger.error("single record size exceeds PMem read buffer. Please increase buffer size.");
            }
            if (bufferPos + length <= capacity) {
                long curKeyPrefix = sortedArray.get(position + 1);
                if (length > bytes.length) {
                    bytes = new byte[length];
                }
                Platform.putLong(bytes, Platform.BYTE_ARRAY_OFFSET, curKeyPrefix);
                Platform.copyMemory(null, curRecordAddress, bytes, Platform.BYTE_ARRAY_OFFSET + Long.BYTES, length - Long.BYTES);
                byteBuffer.put(bytes, 0, length);
                bufferPos += length;
                position += 2;
            } else {
                break;
            }
        }
        return bufferPos;
    }

    @Override
    public boolean hasNext() {
        return (numRecordsRemaining > 0);
    }

    @Override
    public Object getBaseObject() {
        return baseObject;
    }

    @Override
    public long getBaseOffset() {
        return Platform.BYTE_ARRAY_OFFSET;
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
    public void close() {
        // do nothing here
    }
}
