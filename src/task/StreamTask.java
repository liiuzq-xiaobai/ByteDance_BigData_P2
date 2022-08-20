package task;

import common.KeyedState;
import environment.RunTimeEnvironment;
import io.BufferPool;
import io.InputChannel;
import operator.StreamOperator;
import record.CheckPointBarrier;
import record.StreamElement;
import record.StreamRecord;
import record.Watermark;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author kevin.zeng
 * @description 算子的执行实例
 * @create 2022-08-12
 */
public class StreamTask<IN, OUT> extends Thread {
    protected String taskCategory = null;

    //task属于一个运行环境
    protected RunTimeEnvironment environment;

    //当前task生产的数据放到Buffer中
//    protected BufferPool<StreamRecord<OUT>> output;

    //当前task所属的状态存储
    protected KeyedState<String, IN> state;

    protected BufferPool<StreamElement> output;

    //一个task接收一个InputChannel发送的数据
//    protected InputChannel<StreamRecord<IN>> input;

    protected InputChannel<StreamElement> input;

    //一个task可以接收多个InputChannel发送到数据
    protected List<InputChannel<StreamElement>> inputs;

    //task执行算子逻辑
    protected StreamOperator<IN, OUT> mainOperator;

    //task所属的外层节点
    protected ExecutionJobVertex<IN, OUT> vertex;

    protected Watermark systemWatermark;

    //task上游的节点个数，用于barrier对齐
    protected int inputParrellism;

    //记录task已收到的barrier所属task的id
    protected Set<String> barrierSet;

    //task当前持有的barrier
    protected CheckPointBarrier currentBarrier;

    //保存barrier未对齐时接收到的数据
    protected List<StreamElement> checkpointBuffer;

    public StreamTask() {
        barrierSet = new HashSet<>();
        checkpointBuffer = new ArrayList<>();
    }

    public StreamTask(String taskCategory) {
        barrierSet = new HashSet<>();
        checkpointBuffer = new ArrayList<>();
        this.taskCategory = taskCategory;
    }

    public void setMainOperator(StreamOperator<IN, OUT> mainOperator) {
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
        //设置输入管道时，可以得到上游的并行度 等于 为管道提供数据的BufferPool数量
        this.inputParrellism = input.getInputParrellism();
    }

    //设置线程名
    public void name(String name) {
        super.setName(name);
    }

    public String getTaskCategory() {
        return this.taskCategory;
    }

    public void setTaskCategory(String category) {
        this.taskCategory = category;
    }

    //task完成checkpoint操作后，调用该方法通知全局运行环境自己已完成
    public void sendAck() {
        String name = Thread.currentThread().getName();
        System.out.println(name + " send ack");
        environment.receiveAck(name);
    }

    //判断当前task的barrier是否到齐
    public boolean isBarrierAligned() {
        if (barrierSet.size() == inputParrellism) {
            //如果到齐，清空set
            barrierSet.clear();
            return true;
        }
        return false;
    }

    //判断当前task是否持有barrier
    public boolean isCheckpointExists() {
        return !barrierSet.isEmpty();
    }

    //当前barrier为空 或 与当前barrier的id相同的话，说明是同一批下发的checkpoint
    public void setBarrierCount(CheckPointBarrier barrier) {
        if (currentBarrier == null || currentBarrier.getId() == barrier.getId()) {
            if (currentBarrier == null){
                System.out.println(getName() + "【start checkpoint】!!!");
                currentBarrier = barrier;
            }
            barrierSet.add(barrier.getTaskId());
        }
//        if(currentBarrier == null){
//            currentBarrier = barrier;
//        }
        barrierSet.add(barrier.getTaskId());
        System.out.println(getName() + "===update barrier set===" + barrierSet);
    }

    //当barrier到达时，task的处理逻辑
    public void processBarrier(CheckPointBarrier barrier) {
        setBarrierCount(barrier);
        // 等待所有barrier全部到齐，才能执行snapshot
        if (isBarrierAligned()) {
            System.out.println(getName() + "【checkpoint aligned!!】");
            boolean isChecked = mainOperator.snapshotState();
            //如果成功，向全局环境发送ACK
            if (isChecked) sendAck();
            //暂时不测试恢复
//            int i = new Random().nextInt(3);
//            if (i == 2) mainOperator.recoverState();
            //将对齐后的barrier下发(每个算子只会下发1个barrier，但可能会接收多个barrier)
            //真正下发时将barrier的taskid改为当前task的id
            output.push(barrier);
            //将缓存的数据处理后发向下游(缓存数据只会有record和watermark两种类型)
            //缓存的数据都是在此次checkpoint之后到达的！！！
            emitCheckpointBuffer();
            //当前持有的barrier置空
            currentBarrier = null;
        }
    }

    //数据暂存缓冲池的逻辑
    public void temporaryStorage(StreamElement element) {
        System.out.println(getName() + "【cache input data for checkpoint】");
        this.checkpointBuffer.add(element);
    }

    //barrier对齐后将所有缓冲的数据进行处理，并推向下游
    public void emitCheckpointBuffer() {
        System.out.println(getName() + "【处理checkpoint缓冲的数据】" + this.checkpointBuffer.size());
        System.out.println(this.checkpointBuffer);
        for (StreamElement element : checkpointBuffer) {
            if (element.isRecord()) {
                StreamRecord<IN> record = element.asRecord();
                OUT outputValue = mainOperator.processElement(record);
                StreamRecord<OUT> outputData = new StreamRecord<>(outputValue, record.getTimestamp());
                output.push(outputData);
            } else if (element.isWatermark()) {
                output.push(element.asWatermark());
            }
        }
        //最后将缓存清空
        this.checkpointBuffer.clear();
    }
}
