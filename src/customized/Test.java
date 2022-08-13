package customized;

import com.sun.xml.internal.bind.v2.TODO;
import common.ReduceValueState;
import function.KeySelector;
import function.MapFunction;
import function.ReduceFunction;
import io.BufferPool;
import io.InputChannel;
import operator.OneInputStreamOperator;
import operator.StreamMap;
import operator.StreamReduce;
import record.StreamRecord;
import record.Tuple2;
import task.*;

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
        //TODO 以下为DAG图构造过程（此处只用了硬代码）

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
        //算子有几个并行度就会有几个bufferPool，输出数据类型为map
        List<BufferPool<StreamRecord<Map<String,Integer>>>> mapBuffer = new ArrayList<>();
        for (int i = 0; i < parrellism ; i++) {
            BufferPool<StreamRecord<Map<String,Integer>>> pool = new BufferPool<>();
            //map算子的bufferPool要启用分区，以便进行reduce计算
            pool.enablePartition();
            mapBuffer.add(pool);
        }
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
            mapTask.setOutput(mapBuffer.get(i));
            //设置线程名称
            mapTask.name("Map" + i);
            //放入管道集合
            mapChannel.add(input);
            //放入运行实例集合
            mapTaskList.add(mapTask);
        }


        //****连接map和reduce算子
        //创建reduce算子，一个task对应一个，
        //transformation保存并行度，有几个并行度创建几个算子，然后绑定到task
//        OneInputStreamOperator<Map<String, Integer>, Map<String, Integer>, ReduceFunction<Map<String, Integer>>> reducer = new StreamReduce<>(new Reducer());
        List<StreamReduce<Map<String,Integer>>> reducerList = new ArrayList<>();
        //TODO 两个reduce共用一个状态后端，最后的结果其实存在状态后端中
        /*TODO 问题：1.keyBy分区问题尚未解决（要根据String分区，这里是Map，考虑要用Tuple2<String,Integer>结构
                    2.最后结果其实存在状态后端，如何把数据推向下游，且不产生冲突
                    3.分区后，每个算子应该对应一个状态后端？
        */
        ReduceValueState<Map<String,Integer>> reduceValueState = new ReduceValueState<>();
        for (int i = 0; i < 1; i++) {
            /*TODO 当共用一个状态后端时，不能对每一个Task绑定不同的算子，否则做processElement
                更新到ValueState时，出现写入覆盖！！！
            */
            StreamReduce<Map<String, Integer>> reducer = new StreamReduce<>(new Reducer());
            reducer.setValueState(reduceValueState);
            reducerList.add(reducer);
        }
        //reduce算子的运行节点
        ExecutionJobVertex<Map<String,Integer>,Map<String,Integer>> reduceVertex = new ExecutionJobVertex<>();
        //运行节点中包含的运行实例task
        List<StreamTask<Map<String,Integer>,Map<String,Integer>>> reduceTaskList = new ArrayList<>();
        reduceVertex.setTasks(reduceTaskList);
        //reduce算子的channel集合
        List<InputChannel<StreamRecord<Map<String,Integer>>>> reduceChannel = new ArrayList<>();
        //

        //channel输入集合要和上游task的buffer绑定，以实现并行
//        sourceBuffer.bindInputChannel(mapChannel);
        //根据下游的并行度设置InputChannel，这里下游是map算子
        for (int i = 0; i < parrellism; i++) {
            //创建task
            OneInputStateStreamTask<Map<String,Integer>> reduceTask = new OneInputStateStreamTask<>();
//            reduceTask.setMainOperator(reducerList.get(i));
            reduceTask.setMainOperator(reducerList.get(0));
            //创建task的上游输入channel
            InputChannel<StreamRecord<Map<String,Integer>>> input = new InputChannel<>();
            //channel数据的提供者
            input.bindProviderBuffer(mapBuffer.get(i));
            //为channel绑定所属的运行节点
            input.bindExecutionVertex(vertex);
            //TODO ***暂定map的每一个buffer都绑定这两个input管道
            mapBuffer.get(i).bindInputChannel(reduceChannel);
            //输入管道和task绑定
            reduceTask.setInput(input);
            //每个task绑定一个输出池
            reduceTask.setOutput(new BufferPool<>());
            //设置线程名称
            reduceTask.name("Reduce" + i);
            //放入管道集合
            reduceChannel.add(input);
            //放入运行实例集合
            reduceTaskList.add(reduceTask);
        }

        //****开始运行
        for(StreamTask<String, Map<String, Integer>> task : mapTaskList){
            task.start();
        }
        TimeUnit.SECONDS.sleep(3);
        consumer.start();
        TimeUnit.SECONDS.sleep(3);
        for(StreamTask<Map<String,Integer>,Map<String,Integer>> task : reduceTaskList){
            task.start();
        }

        TimeUnit.SECONDS.sleep(60);
        System.out.println(Thread.currentThread().getName() + " 【WordCount】 result: " + reduceValueState.value());
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

class Mappper implements MapFunction<String,Tuple2<String,Integer>>{

    @Override
    public Tuple2<String, Integer> map(String value) {
        Tuple2<String,Integer> result = new Tuple2<>();
        result.f0 = value;
        result.f1 = 1;
        return result;
    }
}

//class Reducer implements ReduceFunction<Tuple2<String,Integer>>{
//
//    @Override
//    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
//
//        return null;
//    }
//}

class Reducer implements ReduceFunction<Map<String,Integer>>{

    @Override
    public Map<String, Integer> reduce(Map<String, Integer> value1, Map<String, Integer> value2) {
        Map<String,Integer> map = new HashMap<>(value1);
        for(Map.Entry<String,Integer> entry : value2.entrySet()){
            String word = entry.getKey();
            int count = entry.getValue();
            map.put(word,map.getOrDefault(word,0)+count);
        }
        return map;
    }
}

//class WordKeySelector implements KeySelector<Tuple2<String,Integer>,String>{
//
//    @Override
//    public String getKey(Tuple2<String, Integer> value) {
//        return value.f0;
//    }
//}
//实现对Map根据Key分区
class WordKeySelector implements KeySelector<Map.Entry<String,Integer>,String>{

    @Override
    public String getKey(Map.Entry<String, Integer> value) {
        return value.getKey();
    }
}