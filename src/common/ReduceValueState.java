package common;

import record.StreamRecord;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public class ReduceValueState<IN> implements ValueState<IN> {
    //状态数据值
    IN value = null;

    @Override
    public IN value() {
        return value;
    }

    //update操作只需要把value存在一个地方即可，flink里叫做状态后端
    @Override
    public synchronized void update(IN value) {
        this.value = value;
    }
}
