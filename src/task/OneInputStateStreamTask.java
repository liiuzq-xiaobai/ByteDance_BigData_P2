package task;

import record.CheckPointBarrier;
import record.StreamElement;
import record.StreamRecord;
import record.Watermark;
import window.WindowAssigner;

import java.util.Random;
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

    private Watermark systemWatermark;

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        while (true) {
            //从InputChannel读取数据
            System.out.println(name + " read from InputChannel");
            StreamRecord<IN> outputData = null;
            System.out.println(name + " processing ....");

            //TODO 指定时间窗口，获取系统时间，如果小于窗口的maxTimeStamp，就继续计算


            StreamElement inputElement = input.take();
            if (inputElement.isRecord()) {
                StreamRecord<IN> inputRecord = inputElement.asRecord();
                //调用处理逻辑
                //watermark过滤掉过期是数据
                if (systemWatermark != null && inputRecord.getTimestamp() < systemWatermark.getTimestamp()) {
                    System.out.println(name + "ignore a expired record");
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
                //TODO 未对齐，数据暂存缓冲池
                if (isBarrierAligend()) output.push(outputData);
                else {
                    //暂存缓冲池逻辑
                    temporaryStorage(outputData);
                }

                System.out.println(name + " write into BufferPool");
            } else if (inputElement.isWatermark()) {
                System.out.println(name + " process 【Watermark】!!");
                systemWatermark = inputElement.asWatermark();

                if (isBarrierAligend()) output.push(systemWatermark);
                else {

                }

            }
            //如果到了时间，将状态后端的所有数据放入buffer
            //放入当前Task的缓冲池，推向下游
//            output.push(outputData);

            //TODO 如果遇到checkpointbarrier，对该task进行状态快照
            //TODO 处理barrier的对齐问题，reduce算子会收到mapParrellism个barrier数据
            //此时快的Barrier到达下游算子后，此Barrier之后到达的数据将会放到缓冲区，不会进行处理。
            //等到其他流慢的Barrier到达后，此算子才进行checkpoint，然后把状态保存到状态后端
            else if (inputElement.isCheckpoint()) {
                CheckPointBarrier barrier = inputElement.asCheckpoint();
                if(name.equals("Reduce2") || name.equals("Reduce1")) continue;
                processBarrier(barrier);
            }
        }
    }


}
