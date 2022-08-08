package api.functions;

import java.io.Serializable;

public interface ReduceFunction<T> extends Function, Serializable {

    /**
     * The core method of ReduceFunction, combining two values into one value of the same type. The
     * reduce function is consecutively applied to all values of a group until only a single value
     * remains.
     *
     * @param value1 The first value to combine.
     * @param value2 The second value to combine.
     * @return The combined value of both input values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    T reduce(T value1, T value2) throws Exception;
}