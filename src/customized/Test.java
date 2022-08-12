package customized;

import function.MapFunction;
import function.ReduceFunction;
import io.BufferPool;
import io.InputChannel;
import operator.OneInputStreamOperator;
import operator.StreamMap;
import record.StreamRecord;
import task.ExecutionJobVertex;
import task.OneInputStreamTask;
import task.SourceStreamTask;
import task.StreamTask;

import java.nio.channels.Channel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class Test {
    static int parrellism = 2;
    //相当于execute中的内容
    public static void main(String[] args) throws InterruptedException {
        //以下为DAG图构造过程（此处只用了硬代码）

        //****连接source和map算子
        //1个source 2个map
        SourceStreamTask consumer = new SourceStreamTask();
        consumer.name("Source");
        //map算子的运行节点
        ExecutionJobVertex<String,Map<String,Integer>> vertex = new ExecutionJobVertex<>();
        //创建map算子
        OneInputStreamOperator<String, Map<String, Integer>, MapFunction<String, Map<String, Integer>>> mapper = new StreamMap<>(new Splitter());
        //运行节点中包含的运行实例task
        List<StreamTask<String,Map<String,Integer>>> mapTaskList = new ArrayList<>();
        vertex.setTasks(mapTaskList);
        //source算子绑定下游输出Buffer
        BufferPool<StreamRecord<String>> sourceBuffer = new BufferPool<>();
        consumer.setOutput(sourceBuffer);
        //map算子的channel集合
        List<InputChannel<StreamRecord<String>>> mapChannel = new ArrayList<>();
        //channel输入集合要和上游task的buffer绑定，以实现并行
        sourceBuffer.bindInputChannel(mapChannel);
        //根据下游的并行度设置InputChannel，这里下游是map算子
        for (int i = 0; i < parrellism; i++) {
            //创建task
            OneInputStreamTask<String,Map<String,Integer>> mapTask = new OneInputStreamTask<>();
            mapTask.setMainOperator(mapper);
            //创建task的上游输入channel
            InputChannel<StreamRecord<String>> input = new InputChannel<>();
            //channel数据的提供者是sourceBuffer
            input.bindProviderBuffer(sourceBuffer);
            //为channel绑定所属的运行节点
            input.bindExecutionVertex(vertex);
            //输入管道和task绑定
            mapTask.setInput(input);
            //每个task绑定一个输出池
            mapTask.setOutput(new BufferPool<>());
            //设置线程名称
            mapTask.name("Map" + i);
            //放入管道集合
            mapChannel.add(input);
            //放入运行实例集合
            mapTaskList.add(mapTask);
        }


        //****连接map和reduce算子
        //创建reduce算子
        OneInputStreamOperator<String, Map<String, Integer>, ReduceFunction<Map<String, Integer>>> reducer = new StreamMap<>(new Splitter());


        //****开始运行
        consumer.start();
        TimeUnit.SECONDS.sleep(5);
        for(StreamTask<String, Map<String, Integer>> task : mapTaskList){
            task.start();
        }

    }
}

class Splitter implements MapFunction<String,Map<String,Integer>>{

    @Override
    public Map<String, Integer> map(String value) {
        Map<String,Integer> map = new HashMap<>();
        String[] str = value.split(" ");
        for (String s : str) {
            map.put(s,map.getOrDefault(s,0)+1);
        }
        return map;
    }
}

class Reducer implements ReduceFunction<Map<String,Integer>>{

    @Override
    public Map<String, Integer> reduce(Map<String, Integer> value1, Map<String, Integer> value2) throws Exception {
        Map<String,Integer> map = new HashMap<>(value1);
        for(Map.Entry<String,Integer> entry : value2.entrySet()){
            String word = entry.getKey();
            int count = entry.getValue();
            map.put(word,map.getOrDefault(word,0)+count);
        }
        return map;
    }
}
