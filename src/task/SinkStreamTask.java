package task;

import io.SinkBufferPool;

import record.CheckPointBarrier;
import record.StreamElement;
import record.StreamRecord;
import utils.SinkUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;


/**
 * @author kevin.zeng
 * @description sink算子：输出到文件
 * @create 2022-08-14
 */
public class SinkStreamTask<IN> extends StreamTask<IN, String> {
    private List<SinkBufferPool> result;
    private SinkBufferPool[] bufferPoolForEachTask;
    int counter = 0;
    private long firstTime;
    private Timestamp startTime;
    private Timestamp endTime;
    //判断sink是否开始工作
    private boolean isStart = false;
    //时间窗口持续时间，单位（秒）
    private int duration;
    public SinkStreamTask(List<SinkBufferPool> result, int inputParallelism) {
        this.result = result;
        this.setTaskCategory("SINK");
        this.inputParrellism = inputParallelism;
        bufferPoolForEachTask = new SinkBufferPool[inputParallelism];
        for (int i = 0; i < inputParallelism; i++) {
            bufferPoolForEachTask[i] = new SinkBufferPool();
        }
    }

    public void setDuration(int duration) {
        this.duration = duration;

    }

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        System.out.println(name + " 开始工作");
        while (true) {
            //从InputChannel读取数据
            StreamElement inputElement = this.input.take();
            //第一次进入循环，设置时间
            if(!isStart){
                this.firstTime = System.currentTimeMillis();
                this.startTime = new Timestamp(firstTime);
                this.endTime = new Timestamp(startTime.getTime() + duration * 1000);
                isStart = true;
            }
            //sink一直在判断现在的时间有没有超过目前的时间窗口
            if (startTime.getTime() == firstTime) {
                SinkUtils.writeTimestamp(startTime, endTime);
                System.out.println("完成第一次时间写入");
                startTime.setTime(System.currentTimeMillis());
            } else if (System.currentTimeMillis() > endTime.getTime()) {
                startTime.setTime(endTime.getTime());
                endTime.setTime(startTime.getTime() + duration * 1000);
                SinkUtils.writeTimestamp(startTime, endTime);
            }
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
                //record的事件事件
                inputRecord.getTimestamp();
                //存进相应分支的缓冲池中
                bufferPool.add(inputRecord);
            } else if (inputElement.isCheckpoint()) {
                System.out.println("test: sink 拿到checkpoint: " + ++counter);
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
                    result.get(index).copyExistingBuffer(bufferPool);
                    bufferPool.getList().clear();
                    //判断result中最前的sinkBufferPool里面已经有多少个checkpoint
                    //如果checkpoint已经够了，那么就把该sinkBufferPool的数据写进文件
                    if (result.get(0).getCheckpointCount() == inputParrellism) {
                        List<? extends StreamElement> list = result.get(0).getList();
                        for (int i = 0; i < list.size(); i++) {
                            StreamElement input = list.get(i);
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
                            } else if (i == list.size() - 1 && input.isCheckpoint()) {
                                result.remove(0);
                            }
                        }
                    }
                    //如果没有，在result中新建一个sinkBufferPool(新一批次的数据)，把 该分支buffer里checkpoint前的record数据 + 该checkpoint 全放进result里面（此时result有checkpoint标记有效批次）
                } else {
                    result.add(new SinkBufferPool());
                    result.get(result.size() - 1).copyExistingBuffer(bufferPool);
                    bufferPool.getList().clear();
                }

            }
        }
    }
}


