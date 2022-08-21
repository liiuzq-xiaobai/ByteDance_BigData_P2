package record;

import java.util.Date;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class StreamRecord<T> extends StreamElement {
    //数据值
    private T value;
    //时间戳
    private long timestamp;

    @Override
    public String toString() {
        return "StreamRecord{" +
                "value=" + value +
                ", timestamp=" + timestamp +
                ", taskId='" + taskId + '\'' +
                '}';
    }

    public StreamRecord() {
        super();
    }

    public StreamRecord(T value){
        super();
        this.value = value;
        this.timestamp = new Date().getTime();
    }

    public StreamRecord(T value, long timestamp) {
        super();
        this.value = value;
        this.timestamp = timestamp;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamRecord<?> that = (StreamRecord<?>) o;

        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}
