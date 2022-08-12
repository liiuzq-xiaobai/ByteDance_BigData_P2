package function;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public interface ReduceFunction<T> extends Function {
    T reduce(T value1, T value2);
}
