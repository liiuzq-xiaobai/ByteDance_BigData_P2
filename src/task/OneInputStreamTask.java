package task;

import function.KeySelector;
import record.CheckPointBarrier;
import record.StreamElement;
import record.StreamRecord;
import record.Watermark;

import java.util.concurrent.TimeUnit;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */

//用于map等算子逻辑
public class OneInputStreamTask<IN,OUT> extends StreamTask<IN,OUT> {
	 public OneInputStreamTask(){
	        super("MAPPER");
	}

    private KeySelector<StreamElement,String> keySelector;
    private Watermark systemWatermark;
    public void setKeySelector(KeySelector<StreamElement, String> keySelector) {
        this.keySelector = keySelector;
    }

    @Override
    public void run(){
        String name = Thread.currentThread().getName();
        while(true){
            //从InputChannel读取数据
            System.out.println(name + " read from InputChannel");
            StreamElement inputElement = input.take();
            String taskId = inputElement.getTaskId();
            //如果是record类型数据
            if(inputElement.isRecord()){
                StreamRecord<IN> inputRecord = inputElement.asRecord();
                //调用处理逻辑
                //watermark系统时间检查
                if(systemWatermark != null && inputRecord.getTimestamp() < systemWatermark.getTimestamp()){
                    System.out.println(name + "---current time---" + systemWatermark.getTimestamp());
                    System.out.println(name + "---ignore a expired record!!!---" + inputRecord);
                    continue;
                }
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
                systemWatermark = inputElement.asWatermark();
                output.push(systemWatermark);
            }else if(inputElement.isCheckpoint()){
                CheckPointBarrier barrier = inputElement.asCheckpoint();
                processBarrier(barrier);
            }
        }
    }
}
