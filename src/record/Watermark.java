package record;

import java.util.Date;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public class Watermark extends StreamElement {
    //时间标识
    private final long timestamp;

    public Watermark(long timestamp) {
        super();
        this.timestamp = timestamp;
    }

    public Watermark(){
        this(new Date().getTime());
    }

    @Override
    public String toString() {
        return "Watermark{" +
                "timestamp=" + timestamp +
                ", taskId='" + taskId + '\'' +
                '}';
    }

    public long getTimestamp() {
        return timestamp;
    }
}
