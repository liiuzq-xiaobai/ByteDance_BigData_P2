package task;

import function.KeySelector;
import record.CheckPoint;
import record.StreamElement;
import record.StreamRecord;
import record.Watermark;

import java.util.concurrent.TimeUnit;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */

//用于处理诸如map、reduce等算子逻辑
public class OneInputStreamTask<IN,OUT> extends StreamTask<IN,OUT> {

    public KeySelector<StreamElement,String> keySelector;

    public void setKeySelector(KeySelector<StreamElement, String> keySelector) {
        this.keySelector = keySelector;
    }

//    public void setKeySelector(KeySelector<StreamRecord<OUT>, String> keySelector) {
//        this.keySelector = keySelector;
//    }

    @Override
    public void run(){
        String name = Thread.currentThread().getName();
        while(true){
            //从InputChannel读取数据
            System.out.println(name + " read from InputChannel");
            StreamElement inputElement = input.take();
            //TODO 如果遇到barrier类型数据，从inputChannel获取消费偏移量
            //如果是record类型数据
            if(inputElement.isRecord()){
                StreamRecord<IN> inputRecord = inputElement.asRecord();
                //调用处理逻辑
                System.out.println(name + " processing ....");
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                OUT outputRecord = mainOperator.processElement(inputRecord);
                StreamRecord<OUT> outputData = new StreamRecord<>(outputRecord,inputRecord.getTimestamp());
                System.out.println(name + " process result: " + outputData);
                //放入当前Task的缓冲池，并推向下游
                output.push(outputData,keySelector);
                System.out.println(name + " write into BufferPool");
            }else if(inputElement.isWatermark()){
                System.out.println(name + " process 【Watermark】!!");
                Watermark watermark = inputElement.asWatermark();
                output.push(watermark);
            }
            //TODO 如果是watermark类型数据

        }
    }
}
