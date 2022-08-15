package window;

import java.util.Collection;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-14
 */
public abstract class WindowAssigner<T, W extends TimeWindow> {

    public abstract Collection<W> assignWindows(T element, long timestamp);
}
