package record;

import java.util.Date;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public class Watermark extends StreamElement {
    private final long timestamp;

    public Watermark(long timestamp) {
        this.timestamp = timestamp;
    }

    public Watermark(){
        this(new Date().getTime());
    }

    @Override
    public String toString() {
        return "Watermark{" +
                "timestamp=" + timestamp +
                '}';
    }
    public long getTimestamp() {
        return timestamp;
    }
}
