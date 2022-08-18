package operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.sun.xml.internal.bind.v2.model.core.TypeRef;
import function.KeySelector;
import function.ReduceFunction;
import record.StreamRecord;
import record.Tuple2;

import java.io.*;
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
        return newValue;
    }

    @Override
    public boolean snapshotState() {
        //在进行checkpoint操作前，需要获取当前的keystate数据

        //将当前的keystate状态存入文件
        //TODO 可优化为异步写入
//        new Thread(() -> {
            String name = Thread.currentThread().getName();
            String path = "checkpoint" + File.separator + name + ".txt";
            System.out.println(name + "【checkpoint state storage】");
            File file = new File(path);
            BufferedWriter writer = null;
            try {
                Collection<T> copyForCheckpoint = copyKeyedState();
                if (!file.exists()) file.createNewFile();
                writer = new BufferedWriter(new FileWriter(file));
                writer.write(String.valueOf(getId()));
                writer.newLine();
                for (T value : copyForCheckpoint) {
                    String str = JSON.toJSONString(value);
                    writer.write(str);
                    writer.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            } finally {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            //TODO 还需要保存checkpoint的唯一标识id
//        }, Thread.currentThread().getName() + "-CKP").start();
        return true;
    }

    @Override
    public void recoverState() {
        //从指定文件中读取最新一次checkpoint保留的state数据
        BufferedReader reader = null;
        //先把keyedstate内容全部清空，再从文件恢复
        //TODO 清空后再恢复可能会影响性能，需要改进
        valueState.clear();
        String name = Thread.currentThread().getName();
        String path = "checkpoint" + File.separator + name + ".txt";
        System.out.println(name + "【recover KeyedState】...");
        try{
            reader = new BufferedReader(new FileReader(path));
            //先读取id，完善后可以删去
            reader.readLine();
            String line="";
            while((line = reader.readLine()) != null){
                T value = (T) JSONObject.parseObject(line,new TypeReference<Tuple2<String,Integer>>(){});
                //恢复到所属的keystate
                valueState.update(value);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println(name + "【recover KeyedState to】" + valueState.get());
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
