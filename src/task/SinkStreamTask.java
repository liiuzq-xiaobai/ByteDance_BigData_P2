package task;

import io.SinkBufferPool;
import record.CheckPointBarrier;
import record.StreamElement;
import record.StreamRecord;
import utils.SinkUtils;

import java.io.IOException;


/**
 * @author kevin.zeng
 * @description sink算子：输出到文件
 * @create 2022-08-14
 */
public class SinkStreamTask<IN> extends StreamTask<IN, String> {
    public SinkBufferPool result;

    public SinkStreamTask(SinkBufferPool result) {
        this.result = result;
        this.setTaskCategory("SINK");
    }

    private int counter = 0;
    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        while (true) {
            //从InputChannel读取数据
            StreamElement inputElement = this.input.take();
            //判断拉取数据的类型
            //当SinkStreamTask拿到record数据，将数据输出到缓冲池
            if (inputElement.isRecord()) {
                System.out.println("test: sink 拿到record");
                StreamRecord<IN> inputRecord = inputElement.asRecord();
                output.add(inputRecord);
                System.out.println(name + "***receive record****");
                System.out.println("result receive Record" + ", right now result size: " + result.getList().size());
                System.out.println("result receive Checkpoint" + ", right now result: " + result.getList());
                //当SinkStreamTask拿到checkpoint数据，意味着需要保存缓冲池的数据到result，
            } else if (inputElement.isCheckpoint()) {
                System.out.println("test: sink 拿到checkpoint: "+ ++counter);
                //如果此时result中已有checkpoint，那么就先将result中的数据写入到output文件
                if (result.isCheckpointExist()) {
                    System.out.println("result 尝试写入文件开始...");
                    for (int i = 0; i < result.getList().size(); i++) {
                        StreamElement input = (StreamElement) result.getList().get(i);
                        if (input.isRecord()) {
                            StreamRecord inputRecord = input.asRecord();
                            //写入文件，并删除result中已写过的数据
                            IN value = (IN) inputRecord.getValue();
                            try {
                                SinkUtils.writeIntoFile("output/wordcount.txt", inputRecord);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            System.out.println(name + " 【print value】" + value);
                        } else if (input.isCheckpoint() && i != 0) {
                            result.getList().clear();
                        }
                    }
                }
                //如果result没有checkpoint或者说result里已完成写入文件，那么将缓冲池的数据保存到result，并且加入checkpoint以标记
                CheckPointBarrier inputCheckpoint = inputElement.asCheckpoint();
                output.add(inputElement);
                System.out.println(name + "***receive Checkpoint****");
                result.copyExistingBuffer(output);
                System.out.println("result receive Checkpoint" + ", right now result size: " + result.getList().size());
                System.out.println("result receive Checkpoint" + ", right now result: " + result.getList());
                //当result完成对缓冲池的copy后，原缓冲池的数据就可以删除了，避免下次再次copy
                output.getList().clear();
            }
        }
    }


}

