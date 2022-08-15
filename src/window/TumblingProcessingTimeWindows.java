package window;

import java.util.Collection;

/**
 * @author kevin.zeng
 * @description 基于处理时间的滚动窗口
 * @create 2022-08-14
 */
public class TumblingProcessingTimeWindows extends WindowAssigner<Object,TimeWindow> {
    private final long size;

    public TumblingProcessingTimeWindows(long size) {
        this.size = size;
    }

    public static TumblingProcessingTimeWindows of(long size) {
        return new TumblingProcessingTimeWindows(size);
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
        return null;
    }
}
