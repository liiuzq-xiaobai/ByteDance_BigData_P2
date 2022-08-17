package io;

import record.StreamElement;

import java.util.ArrayList;

public class SinkBufferPool extends BufferPool{
    public BufferPool<StreamElement> copyExistingBuffer(BufferPool<StreamElement> existingBuffer) {
        return existingBuffer;
    }
    public boolean isCheckpointExist() {
        for (int i = 0; i < this.getList().size(); i++) {
            StreamElement element = (StreamElement) this.getList().get(i);
            if (element.isCheckpoint()) return true;
        }
        return false;
    }


}
