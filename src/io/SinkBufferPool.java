package io;

import record.StreamElement;

//Sink算子输出使用的缓冲池
public class SinkBufferPool<T extends StreamElement> extends BufferPool{
    protected int checkpointCount = 0;
    public void copyExistingBuffer(BufferPool<T> existingBuffer) {
        this.getList().addAll(existingBuffer.getList());
    }
    public boolean isCheckpointExist(int checkpointID) {
        for (int i = 0; i < this.getList().size(); i++) {
            StreamElement element = (StreamElement) this.getList().get(i);
            if (element.isCheckpoint()) {
                return checkpointID == element.asCheckpoint().getCheckpointId();
            }
        }
        return false;
    }
    public void addCheckpoint() {
        checkpointCount++;
    }

    public int getCheckpointCount() {
        return checkpointCount;
    }
}
