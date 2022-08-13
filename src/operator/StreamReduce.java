package operator;

import common.ValueState;
import function.KeySelector;
import function.ReduceFunction;
import record.StreamRecord;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class StreamReduce<T> extends OneInputStreamOperator<T,T, ReduceFunction<T>> {

    ValueState<T> valueState;


    public StreamReduce(ReduceFunction<T> userFunction) {
        super(userFunction);
    }

    public void setValueState(ValueState<T> valueState) {
        this.valueState = valueState;
    }

    @Override
    public T processElement(StreamRecord<T> record) {
        T currentValue = valueState.value();
        T value = record.getValue();
        T newValue;
        //初始值为空，不做操作
        if(currentValue == null){
            newValue = value;
        }else {
            newValue = userFunction.reduce(currentValue, value);
        }
        //更新状态值
        valueState.update(newValue);
        return newValue;
    }
}
