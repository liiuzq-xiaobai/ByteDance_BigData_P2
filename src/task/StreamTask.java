package task;

import common.KeyedState;
import common.ValueState;
import environment.RunTimeEnvironment;
import io.BufferPool;
import io.InputChannel;
import operator.StreamOperator;
import record.StreamElement;
import record.StreamRecord;

import java.util.List;

/**
 * @author kevin.zeng
 * @description 算子的执行实例
 * @create 2022-08-12
 */
public class StreamTask<IN,OUT> extends Thread {

    //task属于一个运行环境
    protected RunTimeEnvironment environment;

    //当前task生产的数据放到Buffer中
//    protected BufferPool<StreamRecord<OUT>> output;

    protected BufferPool<StreamElement> output;

    //一个task接收一个InputChannel发送的数据
//    protected InputChannel<StreamRecord<IN>> input;

    protected InputChannel<StreamElement> input;

    //一个task可以接收多个InputChannel发送到数据
    protected List<InputChannel<StreamElement>> inputs;

    //task执行算子逻辑
    protected StreamOperator<IN,OUT> mainOperator;

    //task所属的外层节点
    protected ExecutionJobVertex<IN,OUT> vertex;

    public StreamTask(){
    }

    public void setMainOperator(StreamOperator<IN,OUT> mainOperator){
        this.mainOperator = mainOperator;
    }

//    public void setOutput(BufferPool<StreamRecord<OUT>> output) {
//        this.output = output;
//    }

    public void setOutput(BufferPool<StreamElement> output) {
        this.output = output;
    }

//    public void setInput(InputChannel<StreamRecord<IN>> input) {
//        this.input = input;
//    }

    public void setInput(InputChannel<StreamElement> input) {
        this.input = input;
    }

    //设置线程名
    public void name(String name){
        super.setName(name);
    }
}
