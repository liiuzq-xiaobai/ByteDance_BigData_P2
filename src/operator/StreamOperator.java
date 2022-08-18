package operator;

import record.StreamRecord;
import task.StreamTask;



/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
//IN为operator输入数据类型，OUT为输出数据类型
public abstract class StreamOperator<IN,OUT> {

    protected StreamTask<?,?> container;

    public abstract OUT processElement(StreamRecord<IN> record);

    //算子存储状态快照的逻辑
    public abstract void snapshotState();
}
