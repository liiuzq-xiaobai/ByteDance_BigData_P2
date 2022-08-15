package window;

import java.util.Collection;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-14
 */
public abstract class WindowAssigner<T> {

    public abstract Collection<TimeWindow> assignWindows(T element, long timestamp);
}
