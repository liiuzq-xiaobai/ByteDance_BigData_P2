package task;

import record.StreamRecord;


/**
 * @author kevin.zeng
 * @description sink算子：输出到文件
 * @create 2022-08-14
 */
public class SinkStreamTask<IN> extends StreamTask<IN,String> {
    @Override
    public void run(){
        String name = Thread.currentThread().getName();
        while(true){
            //从InputChannel读取数据
            System.out.println(name + " read from InputChannel");
            StreamRecord<IN> inputData = input.take();
            //写入文件
            IN value = inputData.getValue();
            System.out.println(name + " 【print value】" + value);
            //放入当前Task的缓冲池，并推向下游（sink不需要推了）
//            output.push(outputData,keySelector);
//            System.out.println(name + " write into BufferPool");
        }
    }
}
