package common;
/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public class ReduceValueState<IN> implements ValueState<IN> {
    //状态数据值
    IN value = null;

    //TODO 这里读的时候加了锁，否则会出现读取时两个线程都读的null，写入时有一个数据会被覆盖
    @Override
    public synchronized IN value() {
        return value;
    }

    //update操作只需要把value存在一个地方即可，flink里叫做状态后端
    @Override
    public synchronized void update(IN value) {
        this.value = value;
        System.out.println(Thread.currentThread().getName() + " update ValueState to " + value);
    }
}
