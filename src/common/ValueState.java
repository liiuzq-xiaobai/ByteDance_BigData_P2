package common;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */

//算子运行时保存状态
public interface ValueState<T> extends State {

    T value();

    void update(T value);
}
