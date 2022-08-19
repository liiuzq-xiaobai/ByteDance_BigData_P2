package io;

import record.CheckPointBarrier;
import record.StreamElement;

import java.util.ArrayList;

public class SinkBufferPool<T extends StreamElement> extends BufferPool{
    public void copyExistingBuffer(BufferPool<T> existingBuffer) {
        this.getList().addAll(existingBuffer.getList());
    }
    public boolean isCheckpointExist() {
        int poolSize = this.getList().size();
        //有可能在读到数据之前读到checkpoint，此时result中没有元素（读到的checkpoint还没放进去）
        if (poolSize == 0) return false;
        //判断result最后一个元素是不是checkpoint
        StreamElement element = (StreamElement) this.getList().get(poolSize - 1);
        return element.isCheckpoint();
    }


}
