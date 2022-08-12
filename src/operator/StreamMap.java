package operator;

import function.MapFunction;
import record.StreamRecord;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class StreamMap<IN,OUT> extends OneInputStreamOperator<IN,OUT,MapFunction<IN,OUT>> {

    public StreamMap(MapFunction<IN, OUT> userFunction) {
        super(userFunction);
    }

    @Override
    public StreamRecord<OUT> processElement(StreamRecord<IN> record) {
        OUT value = userFunction.map(record.getValue());
        return new StreamRecord<>(value);
    }
}
