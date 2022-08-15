package window;

import java.util.Collection;
import java.util.Collections;

/**
 * @author kevin.zeng
 * @description 基于处理时间的滚动窗口
 * @create 2022-08-14
 */
public class TumblingProcessingTimeWindows extends WindowAssigner<Object> {
    private final long size;

    public TumblingProcessingTimeWindows(long size) {
        this.size = size;
    }

    public static TumblingProcessingTimeWindows of(long size) {
        return new TumblingProcessingTimeWindows(size);
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
        final long now = System.currentTimeMillis();
        return Collections.singleton(new TimeWindow(now,now+size));
    }
}
