package task;

import com.sun.xml.internal.bind.v2.TODO;
import io.BufferPool;
import io.SinkBufferPool;
import record.CheckPointBarrier;
import record.StreamElement;
import record.StreamRecord;
import utils.SinkUtils;

import java.io.IOException;
import java.util.List;


/**
 * @author kevin.zeng
 * @description sink算子：输出到文件
 * @create 2022-08-14
 */
public class SinkStreamTask<IN> extends StreamTask<IN, String> {
    public List<SinkBufferPool> result;
    public SinkBufferPool[] bufferPoolForEachTask;

    public SinkStreamTask(List<SinkBufferPool> result, int inputParallelism) {
        this.result = result;
        this.setTaskCategory("SINK");
        this.inputParrellism = inputParallelism;
        bufferPoolForEachTask = new SinkBufferPool[inputParallelism];
        for (int i = 0; i < inputParallelism; i++) {
            bufferPoolForEachTask[i] = new SinkBufferPool();
        }
    }

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        while (true) {
            //从InputChannel读取数据
            StreamElement inputElement = this.input.take();
            //判断数据来自哪个task
            String taskId = inputElement.getTaskId();
            //将taskId进行转换成缓冲池数组索引
            int id = Integer.parseInt(taskId.substring(6));
            //与该数据对应分支的缓冲池
            System.out.println("test: id=" + id);
            SinkBufferPool bufferPool = bufferPoolForEachTask[id];
            //判断拉取数据的类型
            if (inputElement.isRecord()) {
                System.out.println("test: sink 拿到record");
                StreamRecord inputRecord = inputElement.asRecord();
                //存进相应分支的缓冲池中
                bufferPool.add(inputRecord);
            } else if (inputElement.isCheckpoint()) {
                System.out.println("test: sink 拿到checkpoint");
                CheckPointBarrier checkpoint = inputElement.asCheckpoint();
                bufferPool.add(inputElement);
                //遍历result(动态表)所有sinkBufferPool判断result中有没有一个sinkBufferPool有该checkpoint
                int checkpointID = checkpoint.getCheckpointId();
                boolean isCheckpointExist = false;
                int index = 0;
                for (int i = 0; i < result.size(); i++) {
                    if (result.get(i).isCheckpointExist(checkpointID)) {
                        isCheckpointExist = true;
                        index = i;
                        break;
                    }
                }
                if (isCheckpointExist) {
                    //找到对应的sinkBufferPool，把 该分支buffer里checkpoint前的record数据 + 该checkpoint 全放进该sinkBufferPool里面
                    int count = result.get(index).copyExistingBuffer(bufferPool);
                    for (int i = 0; i < count; i++) {
                        bufferPool.getList().remove(0);
                    }
                    //判断result中最前的sinkBufferPool里面已经有多少个checkpoint
                    //如果checkpoint已经够了，那么就把该sinkBufferPool的数据写进文件
                    //再次判断result中最前的sinkBufferPool里面已经有多少个checkpoint，直到result中最前的sinkBufferPool里的checkpoint不够
                    while (!result.isEmpty() && result.get(0).getCheckpointCount() == inputParrellism) {
                        List<? extends StreamElement> list = result.get(0).getList();
                        for (int i = 0; i < list.size(); i++) {
                            StreamElement input = (StreamElement) list.get(i);
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
                            } else if (input.isCheckpoint() && i == list.size() - 1) {
                                result.remove(0);
                            }
                        }
                    }
                    //如果没有，在result中新建一个sinkBufferPool(新一批次的数据)，把 该分支buffer里checkpoint前的record数据 + 该checkpoint 全放进result里面（此时result有checkpoint标记有效批次）
                } else {
                    result.add(new SinkBufferPool());
                    int count = result.get(result.size() - 1).copyExistingBuffer(bufferPool);
                    for (int i = 0; i < count; i++) {
                        bufferPool.getList().remove(0);
                    }
                }

            }
        }
    }
}


