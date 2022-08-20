package record;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public class StreamElement {

    protected String taskId;

    public StreamElement(){
    }
    public final boolean isRecord() {
        return getClass() == StreamRecord.class;
    }

    public final <T> StreamRecord<T> asRecord() {
        return (StreamRecord<T>) this;
    }

    public final boolean isWatermark() {
        return getClass() == Watermark.class;
    }

    public final Watermark asWatermark() {
        return (Watermark) this;
    }

    public final boolean isCheckpoint() {
        return getClass() == CheckPointBarrier.class;
    }
    public final CheckPointBarrier asCheckpoint() {
        return (CheckPointBarrier) this;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
}
