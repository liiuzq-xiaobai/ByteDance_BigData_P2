package task;

import record.StreamElement;
import record.StreamRecord;
import record.Watermark;
import window.WindowAssigner;

import java.util.concurrent.TimeUnit;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class OneInputStateStreamTask<IN> extends StreamTask<IN, IN> {
    WindowAssigner<StreamElement> windowAssigner;
    public OneInputStateStreamTask(){
        super("REDUCER");
    }

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
                Watermark watermark = inputElement.asWatermark();
                output.push(watermark);
            }
            //如果到了时间，将状态后端的所有数据放入buffer
            //放入当前Task的缓冲池，推向下游
//            output.push(outputData);
        }
    }


}
