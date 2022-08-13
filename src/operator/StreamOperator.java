package operator;

import record.StreamRecord;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
//IN为operator输入数据类型，OUT为输出数据类型
public abstract class StreamOperator<IN,OUT> {
    public abstract OUT processElement(StreamRecord<IN> record);
}
