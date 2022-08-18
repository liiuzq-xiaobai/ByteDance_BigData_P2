package task;

import io.SinkBufferPool;
import record.CheckPointBarrier;
import record.StreamElement;
import record.StreamRecord;
import utils.SinkUtils;

import java.io.IOException;
import java.sql.Time;
import java.util.concurrent.TimeUnit;


/**
 * @author kevin.zeng
 * @description sink算子：输出到文件
 * @create 2022-08-14
 */
public class SinkStreamTask<IN> extends StreamTask<IN,String> {
    public SinkBufferPool result;

    public SinkStreamTask(SinkBufferPool result) {
        this.result = result;
    }

    @Override
    public void run(){
        String name = Thread.currentThread().getName();
        while(true){
            //从InputChannel读取数据
            System.out.println(name + " read from InputChannel");
            StreamElement inputElement = this.input.take();
            //判断拉取数据的类型
            //当SinkStreamTask拉到record数据，将数据输出到缓冲池
            if(inputElement.isRecord()){
                StreamRecord<IN> inputRecord = inputElement.asRecord();
                output.add(inputRecord);
            //当SinkStreamTask拉到checkpoint数据，意味着需要保存缓冲池的数据到result，
            } else if (inputElement.isCheckpoint()) {
                System.out.println(name + "***receive Checkpoint****");
                //如果此时result中已有checkpoint，那么就先将result中的数据写入到output文件
                if (result.isCheckpointExist()) {
                    for (int i = 0; i < result.getList().size(); i++) {
                        StreamElement input = (StreamElement) result.getList().get(i);
                        if (input.isCheckpoint()) {
                            result.getList().remove(input);
                        } else if (input.isRecord()) {
                            StreamRecord inputRecord = (StreamRecord) input;
                            //写入文件，并删除result中已写过的数据
                            IN value = (IN) inputRecord.getValue();
                            try {
                                SinkUtils.writeIntoFile("output/wordcount.txt",inputRecord);
                                result.getList().remove((StreamElement) inputRecord);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            System.out.println(name + " 【print value】" + value);
                        }
                    }
                }
                //如果result没有checkpoint，那么将缓冲池的数据保存到result，并且加入checkpoint以标记
                CheckPointBarrier inputCheckpoint = inputElement.asCheckpoint();
                output.add(inputElement);
                result.copyExistingBuffer(output);
                //当result完成对缓冲池的copy后，原缓冲池的数据就可以删除了，避免下次再次copy
                output.getList().clear();
            }
            //放入当前Task的缓冲池，并推向下游（sink不需要推了）
//            output.push(outputData,keySelector);
//            System.out.println(name + " write into BufferPool");
        }
    }


}

