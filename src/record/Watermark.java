package record;

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
}
