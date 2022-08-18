package operator;

import function.KeySelector;
import function.ReduceFunction;
import record.StreamRecord;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class StreamReduce<T> extends OneInputStreamOperator<T, T, ReduceFunction<T>> {

    private static int idCounter = 0;

    public int getId() {
        return idCounter++;
    }

    public StreamReduce(ReduceFunction<T> userFunction) {
        this(userFunction, null);
    }

    public StreamReduce(ReduceFunction<T> userFunction, KeySelector<T, String> keySelector) {
        super(userFunction, keySelector);

    }


    @Override
    public T processElement(StreamRecord<T> record) {

        T value = record.getValue();
        T newValue;
//        synchronized (valueState){
        String key = keySelector.getKey(value);
        T currentValue = valueState.value(key);
        //初始值为空，不做操作
        if (currentValue == null) {
            newValue = value;
        } else {
            newValue = userFunction.reduce(currentValue, value);
        }
        //更新状态值
        valueState.update(newValue);
//        }
        return newValue;
    }

    @Override
    public void snapshotState() {
        //TODO 存储该算子所持有的keystate的当前数据
        //在进行checkpoint操作前，需要获取当前的keystate数据
        Collection<T> copyForCheckpoint = copyKeyedState();
        //将当前的keystate状态存入文件，异步
//        new Thread(() -> {
            String name = Thread.currentThread().getName();
            String path = "checkpoint" + File.separator + name + ".txt";
            System.out.println(name + "【checkpoint state storage】");
            File file = new File(path);
            BufferedWriter writer = null;
            try {
                if (!file.exists()) file.createNewFile();
                writer = new BufferedWriter(new FileWriter(file));
                writer.write(String.valueOf(getId()));
                writer.newLine();
                for (T value : copyForCheckpoint) {
                    writer.write(value.toString());
                    writer.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            //TODO 还需要保存checkpoint的id
//        }, Thread.currentThread().getName() + "-CKP").start();
    }

    public Collection<T> copyKeyedState() {
        //由于进行快照时，keystate可能还会去处理数据，因此获取状态数据复制时要对keystate加锁
        Collection<T> current;
        Collection<T> copyForCheckpoint = new ArrayList<>();
        synchronized (valueState) {
            current = valueState.get();
            copyForCheckpoint.addAll(current);
        }
        return copyForCheckpoint;
    }

}
