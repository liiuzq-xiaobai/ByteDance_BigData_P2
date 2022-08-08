package api.operators;

import runtime.streamrecord.StreamRecord;

public interface OneInputStreamOperator<IN, OUT> extends StreamOperator<OUT>, Input<IN> {
    default void setKeyContextElement(StreamRecord<IN> record) throws Exception {
        this.setKeyContextElement1(record);
    }
}
