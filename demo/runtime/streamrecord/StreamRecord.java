package runtime.streamrecord;

public final class StreamRecord<T> extends StreamElement {
    private T value;
    private long timestamp;
    private boolean hasTimestamp;

    public StreamRecord(T value) {
        this.value = value;
    }

    public StreamRecord(T value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
        this.hasTimestamp = true;
    }
}
