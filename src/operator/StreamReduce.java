package operator;

import common.KeyedState;
import function.KeySelector;
import function.ReduceFunction;
import record.StreamRecord;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class StreamReduce<T> extends OneInputStreamOperator<T, T, ReduceFunction<T>> {


    public StreamReduce(ReduceFunction<T> userFunction) {
        this(userFunction, null);
    }

    public StreamReduce(ReduceFunction<T> userFunction, KeySelector<T, String> keySelector) {
        super(userFunction, keySelector);
    }


    @Override
    public T processElement(StreamRecord<T> record) {

        T value = record.getValue();
        T newValue;
        /* TODO 共用valueState时，valueState是多个task的共享资源，操作时需要加锁
                如果每个task独享一个valueState，则不需要加锁，但暂时采取共用的方法
         */
//        synchronized (valueState){
        String key = keySelector.getKey(value);
        T currentValue = valueState.value(key);
        //初始值为空，不做操作
        if (currentValue == null) {
            newValue = value;
        } else {
            newValue = userFunction.reduce(currentValue, value);
        }
        //更新状态值
        valueState.update(newValue);
//        }
        return newValue;
    }
}
