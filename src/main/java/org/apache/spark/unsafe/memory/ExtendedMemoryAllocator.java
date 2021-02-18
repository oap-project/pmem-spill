
package org.apache.spark.unsafe.memory;
import com.intel.oap.common.unsafe.PersistentMemoryPlatform;

public class ExtendedMemoryAllocator implements MemoryAllocator{

    @Override
    public MemoryBlock allocate(long size) throws OutOfMemoryError {
        long address = PersistentMemoryPlatform.allocateVolatileMemory(size);
        MemoryBlock memoryBlock = new MemoryBlock(null, address, size);

        return memoryBlock;
    }

    @Override
    public void free(MemoryBlock memoryBlock) {
        assert (memoryBlock.getBaseObject() == null) :
                "baseObject not null; are you trying to use the AEP-heap allocator to free on-heap memory?";
        PersistentMemoryPlatform.freeMemory(memoryBlock.getBaseOffset());
    }
}