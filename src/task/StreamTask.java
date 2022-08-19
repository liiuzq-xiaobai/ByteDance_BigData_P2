package task;

import common.KeyedState;
import environment.RunTimeEnvironment;
import environment.StreamExecutionEnvironment;
import io.BufferPool;
import io.InputChannel;
import operator.StreamOperator;
import record.StreamElement;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

/**
 * @author kevin.zeng
 * @description 算子的执行实例
 * @create 2022-08-12
 */
public class StreamTask<IN,OUT> extends Thread {
	protected String taskCategory = null;

    //task属于一个运行环境
    protected RunTimeEnvironment environment;

    //当前task生产的数据放到Buffer中
//    protected BufferPool<StreamRecord<OUT>> output;

    //当前task所属的状态存储
    protected KeyedState<String,IN> state;

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
    
    public StreamTask(String taskCategory){
    	this.taskCategory = taskCategory;
    }

    public void setMainOperator(StreamOperator<IN,OUT> mainOperator){
        this.mainOperator = mainOperator;
    }

    public BufferPool<StreamElement> getOutput() {
        return output;
    }

    public void setOutput(BufferPool<StreamElement> output) {
        this.output = output;
    }

    public void setEnvironment(RunTimeEnvironment environment) {
        this.environment = environment;
    }

    public void setInput(InputChannel<StreamElement> input) {
        this.input = input;
    }

    //设置线程名
    public void name(String name){
        super.setName(name);
    }
    
    public String getTaskCategory() {
    	return this.taskCategory;
    }
    
    public void setTaskCategory(String category) {
    	this.taskCategory = category;
    }

    //task完成checkpoint操作后，调用该方法通知全局运行环境自己已完成
    public void sendAck(){
        String name = Thread.currentThread().getName();
        System.out.println(name + " send ack");
        environment.receiveAck(name);
    }
}
