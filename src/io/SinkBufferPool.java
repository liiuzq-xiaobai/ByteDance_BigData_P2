package io;

import record.CheckPointBarrier;
import record.StreamElement;

import java.util.ArrayList;
import java.util.List;

public class SinkBufferPool<T extends StreamElement> extends BufferPool{
    protected int checkpointCount = 0;
//    public int copyExistingBuffer(BufferPool<T> existingBuffer) {
//        checkpointCount++;
//        int count = 0;
//        List<? extends StreamElement> list = existingBuffer.getList();
//        for (int i = 0; i < list.size(); i++) {
//            StreamElement element = list.get(i);
//            if (element.isCheckpoint()) {
//                this.getList().add(element);
//                count++;
//                break;
//            }
//            this.getList().add(element);
//            count++;
//        }
//        return count;
//    }
    public void copyExistingBuffer(BufferPool<T> existingBuffer) {
        this.getList().addAll(existingBuffer.getList());
//        return existingBuffer.getList().size();
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
