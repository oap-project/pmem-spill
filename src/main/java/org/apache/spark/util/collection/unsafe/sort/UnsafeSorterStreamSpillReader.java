package org.apache.spark.util.collection.unsafe.sort;

import com.google.common.io.Closeables;
import com.intel.oap.common.storage.stream.ChunkInputStream;
import com.intel.oap.common.storage.stream.DataStore;
import org.apache.spark.SparkEnv;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.ReadAheadInputStream;
import org.apache.spark.memory.PMemManagerInitializer;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockId;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class UnsafeSorterStreamSpillReader extends UnsafeSorterSpillReader {

  protected final ChunkInputStream chunkInputStream;
  public UnsafeSorterStreamSpillReader(
      SerializerManager serializerManager,
      TaskMetrics taskMetrics,
      File file,
      BlockId blockId) throws IOException {
    super(taskMetrics);
    final ConfigEntry<Object> bufferSizeConfigEntry =
            package$.MODULE$.UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE();
    // This value must be less than or equal to MAX_BUFFER_SIZE_BYTES. Cast to int is always safe.
    final int DEFAULT_BUFFER_SIZE_BYTES =
            ((Long) bufferSizeConfigEntry.defaultValue().get()).intValue();
    int bufferSizeBytes = SparkEnv.get() == null ? DEFAULT_BUFFER_SIZE_BYTES :
            ((Long) SparkEnv.get().conf().get(bufferSizeConfigEntry)).intValue();
    final boolean readAheadEnabled = SparkEnv.get() != null && (boolean)SparkEnv.get().conf().get(
            package$.MODULE$.UNSAFE_SORTER_SPILL_READ_AHEAD_ENABLED());
    chunkInputStream =
            ChunkInputStream.getChunkInputStreamInstance(file.toString(),
                    new DataStore(PMemManagerInitializer.getPMemManager(),
                            PMemManagerInitializer.getProperties()));
    final InputStream bs = chunkInputStream;
    try {
        if (readAheadEnabled) {
            this.in = new ReadAheadInputStream(serializerManager.wrapStream(blockId, bs),
                    bufferSizeBytes);
        } else {
            this.in = serializerManager.wrapStream(blockId, bs);
        }
        this.din = new DataInputStream(this.in);
        numRecords = numRecordsRemaining = din.readInt();
    } catch (IOException e) {
        Closeables.close(bs, /* swallowIOException = */ true);
        throw e;
    }
  }
}
