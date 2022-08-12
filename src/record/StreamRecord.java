package record;

import java.util.Date;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class StreamRecord<T> {

    private T value;

    private long timestamp;

    @Override
    public String toString() {
        return "StreamRecord{" +
                "value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }

    public StreamRecord() {
    }

    public StreamRecord(T value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public StreamRecord(T value){
        this.value = value;
        this.timestamp = new Date().getTime();
    }
    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


}
