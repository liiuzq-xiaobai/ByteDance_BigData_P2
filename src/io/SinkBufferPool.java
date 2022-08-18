package io;

import record.CheckPointBarrier;
import record.StreamElement;

import java.util.ArrayList;

public class SinkBufferPool extends BufferPool{
    public BufferPool<StreamElement> copyExistingBuffer(BufferPool<StreamElement> existingBuffer) {
        return existingBuffer;
    }
    public boolean isCheckpointExist() {
        int poolSize = this.getList().size();
        //判断result最后一个元素是不是checkpoint
        //TODO 有可能已经没有数据读了，只有checkpoint在不停发送了，此时就会get（-1），以后再考虑
        return this.getList().get(poolSize - 1).getClass() == CheckPointBarrier.class;
    }


}
