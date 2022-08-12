package operator;

import function.ReduceFunction;
import record.StreamRecord;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class StreamReduce<T> extends OneInputStreamOperator<T,T, ReduceFunction<T>> {

    public StreamReduce(ReduceFunction<T> userFunction) {
        super(userFunction);
    }

    @Override
    public StreamRecord<T> processElement(StreamRecord<T> record) {
//        userFunction.reduce();
        return null;
    }
}
