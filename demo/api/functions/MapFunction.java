package api.functions;

import java.io.Serializable;

public interface MapFunction<T, O> extends Function, Serializable {
    /**
     * The mapping method. Takes an element from the input data set and transforms it into exactly
     * one element.
     *
     * @param value The input value.
     * @return The transformed value
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    O map(T value) throws Exception;
}
