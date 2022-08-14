package window;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-14
 */
public class TimeWindow {
    private final long start;
    private final long end;

    public TimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public long maxTimestamp() {
        return end - 1;
    }
}
