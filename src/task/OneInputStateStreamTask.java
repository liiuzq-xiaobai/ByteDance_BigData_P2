package task;

import record.CheckPointBarrier;
import record.StreamElement;
import record.StreamRecord;
import window.WindowAssigner;

import java.util.concurrent.TimeUnit;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
//处理带状态的算子逻辑，如reduce
public class OneInputStateStreamTask<IN> extends StreamTask<IN, IN> {

    WindowAssigner<StreamElement> windowAssigner;

    public OneInputStateStreamTask() {
        super("REDUCER");
    }

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        while (true) {
            //从InputChannel读取数据
            System.out.println(name + " read from InputChannel and processing");
            StreamRecord<IN> outputData = null;

            //TODO 指定时间窗口，获取系统时间，如果小于窗口的maxTimeStamp，就继续计算

            StreamElement inputElement = input.take();
            String taskId = inputElement.getTaskId();
            System.out.println(name + "receive " + taskId);
            //将该数据表明是由哪个reduce分支向下游sink发送的，方便后期sink算子判断
            //这一步在bufferpool的push里面做了
//            inputElement.setTaskId(name);
            //如果当前task没有处于checkpoint状态，向下游发送处理过后的数据
            if (!isCheckpointExists()) {
                System.out.println(name + "***not in checkpoint status***");
                if (inputElement.isRecord()) {
                    StreamRecord<IN> inputRecord = inputElement.asRecord();
                    //调用处理逻辑
                    //watermark过滤掉过期是数据
                    if (systemWatermark != null && inputRecord.getTimestamp() < systemWatermark.getTimestamp()) {
                        System.out.println(name + "---current time---" + systemWatermark.getTimestamp());
                        System.out.println(name + "---ignore a expired record!!!---" + inputRecord);
                        continue;
                    }

                    //如果是有状态的算子（如reduce，需要从状态中取初值，再跟输入值计算）
                    IN outputRecord = mainOperator.processElement(inputRecord);
                    //和输入数据采取相同的时间（事件时间）
                    outputData = new StreamRecord<>(outputRecord, inputRecord.getTimestamp());
                    try {
                        TimeUnit.SECONDS.sleep(3);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(name + " process result: " + outputData);
                    output.push(outputData);
                    System.out.println(name + " write into BufferPool");
                } else if (inputElement.isWatermark()) {
                    System.out.println(name + " process 【Watermark】!!");
                    systemWatermark = inputElement.asWatermark();
                    output.push(systemWatermark);
                }
                //如果到了时间，将状态后端的所有数据放入buffer
                //放入当前Task的缓冲池，推向下游

                //如果遇到checkpointbarrier，对该task进行状态快照
                //此时快的Barrier到达下游算子后，此Barrier之后到达的数据将会放到缓冲区，不会进行处理。
                //等到其他流慢的Barrier到达后，此算子才进行checkpoint，然后把状态保存到状态后端
                else if (inputElement.isCheckpoint()) {
                    //并行度为1时相当于直接下发
                    CheckPointBarrier barrier = inputElement.asCheckpoint();
                    System.out.println("+++" + barrier);
                    System.out.println(name + "【receive checkpoint; count: 1");
                    processBarrier(barrier);
                }
            }
            //如果当前task处于checkpoint状态，
            else {
                //如果输入数据为barrier类型，调用处理barrier的方法
                if (inputElement.isCheckpoint()) {
                    //如果此次的checkpoint与当前task持有的checkpoint不相同，直接丢弃
                    CheckPointBarrier barrier = inputElement.asCheckpoint();
                    //设置taskId为上游算子的线程名，只有下发barrier时，才把id设为当前task的id
                    if(barrier.getId() != currentBarrier.getId()){
                        System.out.println(name + "【checkpoint conflict!】");
                        continue;
                    }
                    System.out.println("+++" + barrier);
                    System.out.println(name + "【receive checkpoint; count: "+(barrierSet.size()+1));
                    processBarrier(barrier);
                }
                //非barrier类型，缓存起来
                //对于taskId已存在于barrierSet的task，该task在对齐前发送的数据全部加入缓冲池
                //对于taskId不存在于barrierSet的task，即该task的checkpoint还未到达，数据处理后继续下发数据
                else {
                    if(barrierSet.contains(taskId)){
                        System.out.println("已经接收到【" + taskId +"】的barrier，暂存数据");
                        temporaryStorage(inputElement);
                    }else {
                        System.out.println("未接收到【" + taskId +"】的barrier，继续下发数据");
                        //进行数据处理
                        if (inputElement.isRecord()) {
                            StreamRecord<IN> inputRecord = inputElement.asRecord();
                            //调用处理逻辑
                            //watermark过滤掉过期是数据
                            if (systemWatermark != null && inputRecord.getTimestamp() < systemWatermark.getTimestamp()) {
                                System.out.println(name + "---current time---" + systemWatermark.getTimestamp());
                                System.out.println(name + "---ignore a expired record!!!---" + inputRecord);
                                continue;
                            }

                            //如果是有状态的算子（如reduce，需要从状态中取初值，再跟输入值计算）
                            IN outputRecord = mainOperator.processElement(inputRecord);
                            //和输入数据采取相同的时间（事件时间）
                            outputData = new StreamRecord<>(outputRecord, inputRecord.getTimestamp());
                            try {
                                TimeUnit.SECONDS.sleep(3);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            System.out.println(name + " process result: " + outputData);
                            output.push(outputData);
                            System.out.println(name + " write into BufferPool");
                        } else if (inputElement.isWatermark()) {
                            System.out.println(name + " process 【Watermark】!!");
                            systemWatermark = inputElement.asWatermark();
                            output.push(systemWatermark);
                        }
                    }

                }
            }


        }
    }


}
