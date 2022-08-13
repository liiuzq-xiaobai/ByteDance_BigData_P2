package task;

import record.StreamRecord;

import java.util.concurrent.TimeUnit;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class OneInputStateStreamTask<IN> extends StreamTask<IN, IN> {

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        while (true) {
            //从InputChannel读取数据
            System.out.println(name + " read from InputChannel");
            StreamRecord<IN> outputData = null;
            System.out.println(name + " processing ....");
            //计算三次在输出结果（后面需要修改）
//            for (int i = 0; i < 2; i++) {
            StreamRecord<IN> inputData = input.take();
            //调用处理逻辑
            //如果是有状态的算子（如reduce，需要从状态中取初值，再跟输入值计算）
            //计算一段时间再输出给下游
            IN outputRecord = mainOperator.processElement(inputData);
            outputData = new StreamRecord<>(outputRecord);
//            }
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(name + " process result: " + outputData);
            //放入当前Task的缓冲池，推向下游
//            output.add(outputData);
            output.push(outputData);
            System.out.println(name + " write into BufferPool");
        }
    }


}
