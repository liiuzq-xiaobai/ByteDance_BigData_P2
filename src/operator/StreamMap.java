package operator;

import function.KeySelector;
import function.MapFunction;
import record.StreamRecord;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class StreamMap<IN,OUT> extends OneInputStreamOperator<IN,OUT,MapFunction<IN,OUT>> {

    public StreamMap(MapFunction<IN, OUT> userFunction) {
        this(userFunction,null);
    }

    //根据输出结果选择key，向下游指定管道下发
    public StreamMap(MapFunction<IN, OUT> userFunction, KeySelector<OUT,String> keySelector) {
        super(userFunction,keySelector);
    }
    @Override
    public OUT processElement(StreamRecord<IN> record) {
        OUT value = userFunction.map(record.getValue());
        return value;
    }

    @Override
    public void snapshotState() {
    }

    @Override
    public void recoverState() {

    }

}
