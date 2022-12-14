package main;

import utils.KeySelectorWrapper;
import state.MapKeyedState;
import environment.RunTimeEnvironment;
import function.KeySelector;
import function.MapFunction;
import function.ReduceFunction;
import io.BufferPool;
import io.InputChannel;
import io.SinkBufferPool;
import operator.OneInputStreamOperator;
import operator.StreamMap;
import operator.StreamReduce;
import record.StreamElement;
import record.Tuple2;
import task.*;

import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class MainProgram {
    private static final int DEFAULT_PARRELLISM = Runtime.getRuntime().availableProcessors();
    //用户自定义Function类存放的包
    private static final String CLIENT_CLASS_PATH = "customized.";
    //用户Properties文件路径
    private static final String CLIENT_FILE_PATH = "src" + File.separator + "customized" + File.separator + "config.properties";

    public static void main(String[] args) throws Exception {
    	//****创建全局运行环境
        RunTimeEnvironment environment = new RunTimeEnvironment();

        //****读取配置文件
        Properties props = new Properties();
        props.load(new FileReader(CLIENT_FILE_PATH));
        int mapParrellism = props.get("map.parrellism") == null ? DEFAULT_PARRELLISM : Integer.valueOf(String.valueOf(props.get("map.parrellism")));
        int reduceParrellism = props.get("reduce.parrellism") == null ? DEFAULT_PARRELLISM : Integer.valueOf(String.valueOf(props.get("reduce.parrellism")));
        String keySelectorClassName = String.valueOf(props.get("keySelector.class"));
        String mapFunctionClassName = String.valueOf(props.get("mapFunction.class"));
        String reduceFunctionClassName = String.valueOf(props.get("reduceFunction.class"));
        int duration = Integer.valueOf(String.valueOf(props.get("window.size")));

        //反射创建key选择器
        Class keySelectorClass = Class.forName(CLIENT_CLASS_PATH + keySelectorClassName);
        KeySelector<Tuple2<String, Integer>, String> keySelector = (KeySelector<Tuple2<String, Integer>, String>) keySelectorClass.newInstance();
        //将key选择器转换为StreamElement包装的形式
        KeySelectorWrapper<Tuple2<String, Integer>> wrapper = new KeySelectorWrapper<>();
        KeySelector<StreamElement, String> keySelector1 = wrapper.convert(keySelector);

        //反射创建用户MapFunction
        Class mapFunctionClass = Class.forName(CLIENT_CLASS_PATH + mapFunctionClassName);
        MapFunction<String, Tuple2<String, Integer>> mapFunction = (MapFunction<String, Tuple2<String, Integer>>) mapFunctionClass.newInstance();

        //反射创建用户ReduceFunction
        Class reduceFunctionClass = Class.forName(CLIENT_CLASS_PATH + reduceFunctionClassName);
        ReduceFunction<Tuple2<String, Integer>> reduceFunction = (ReduceFunction<Tuple2<String, Integer>>) reduceFunctionClass.newInstance();

        //以下为DAG图构造过程

        //****连接source和map算子
        SourceStreamTask consumer = new SourceStreamTask();
        consumer.name("Source");

        //map算子的运行节点
        ExecutionJobVertex<String, Tuple2<String, Integer>> vertex = new ExecutionJobVertex<>();
        //创建map算子，声明key选择器
        OneInputStreamOperator<String, Tuple2<String, Integer>, MapFunction<String, Tuple2<String, Integer>>> mapper = new StreamMap<>(mapFunction);
        environment.setMapper(mapper);
        //运行节点中包含的运行实例task
        List<StreamTask<String, Tuple2<String, Integer>>> mapTaskList = new ArrayList<>();
        vertex.setTasks(mapTaskList);
        environment.setMapTaskList(mapTaskList);
        //source算子绑定下游输出Buffer
        BufferPool<StreamElement> sourceBuffer = new BufferPool<>();
        consumer.setOutput(sourceBuffer);
        environment.setSourceBuffer(sourceBuffer);
        //map算子的channel集合
        List<InputChannel<StreamElement>> mapChannel = new ArrayList<>();
        //channel输入集合要和上游task的buffer绑定，以实现并行
        sourceBuffer.bindInputChannel(mapChannel);
        //算子有几个并行度就会有几个bufferPool，输出数据类型为map
        List<BufferPool<StreamElement>> mapBuffer = new ArrayList<>();
        for (int i = 0; i < mapParrellism; i++) {
            BufferPool<StreamElement> pool = new BufferPool<>();
            mapBuffer.add(pool);
        }
        //根据下游的并行度设置InputChannel，这里下游是map算子
        for (int i = 0; i < mapParrellism; i++) {
            //创建task
            OneInputStreamTask<String, Tuple2<String, Integer>> mapTask = new OneInputStreamTask<>();
            mapTask.setMainOperator(mapper);
            //创建task的上游输入channel
            InputChannel<StreamElement> input = new InputChannel<>();
            //channel数据的提供者是sourceBuffer
            input.bindProviderBuffer(Collections.singletonList(sourceBuffer));
            //为channel绑定所属的运行节点
            input.bindExecutionVertex(vertex);
            //输入管道和task绑定
            mapTask.setInput(input);
            //每个task绑定一个输出池
            mapTask.setOutput(mapBuffer.get(i));
            //设置线程名称
            mapTask.name("Map" + i);
            //每个task绑定key选择器，用于分发数据
            mapTask.setKeySelector(keySelector1);
            //放入管道集合
            mapChannel.add(input);
            //放入运行实例集合
            mapTaskList.add(mapTask);
            environment.addTask(mapTask);
        }
        
        environment.setMapChannel(mapChannel);
        environment.setMapBuffer(mapBuffer);
        environment.setKeySelector1(keySelector1);


        //****连接map和reduce算子
        //创建reduce算子，一个task对应一个，
        //transformation保存并行度，有几个并行度创建几个算子，然后绑定到task
        List<StreamReduce<Tuple2<String, Integer>>> reducerList = new ArrayList<>();
        for (int i = 0; i < reduceParrellism; i++) {
            StreamReduce<Tuple2<String, Integer>> reducer = new StreamReduce<>(reduceFunction, keySelector);
            //按key分区后，对每个task绑定不同的算子，每个算子独占一个状态后端
            reducer.setValueState(new MapKeyedState<>(keySelector));
            reducerList.add(reducer);
        }
        environment.setReducerList(reducerList);
        //reduce算子的运行节点
        ExecutionJobVertex<Tuple2<String, Integer>, Tuple2<String, Integer>> reduceVertex = new ExecutionJobVertex<>();
        //运行节点中包含的运行实例task
        List<StreamTask<Tuple2<String, Integer>, Tuple2<String, Integer>>> reduceTaskList = new ArrayList<>();
        reduceVertex.setTasks(reduceTaskList);
        environment.setReduceTaskList(reduceTaskList);
        //reduce算子的channel集合
        List<InputChannel<StreamElement>> reduceChannel = new ArrayList<>();
        for (int i = 0; i < mapParrellism; i++) {
            //map的每一个buffer都绑定reduce算子的所有input管道，按哈希值%管道数进行shuffle
            mapBuffer.get(i).bindInputChannel(reduceChannel);
        }
        environment.setReduceChannel(reduceChannel);
        //
        //reduce算子的buffer集合
        List<BufferPool<StreamElement>> reduceBuffer = new ArrayList<>();
        environment.setReduceBuffer(reduceBuffer);
        //channel输入集合要和上游task的buffer绑定，以实现并行
        //根据下游的并行度设置InputChannel
        for (int i = 0; i < reduceParrellism; i++) {
            //创建task
            OneInputStateStreamTask<Tuple2<String, Integer>> reduceTask = new OneInputStateStreamTask<>();
            reduceTask.setMainOperator(reducerList.get(i));
            //创建task的上游输入channel
            InputChannel<StreamElement> input = new InputChannel<>();
            //channel数据的提供者
            input.bindProviderBuffer(mapBuffer);
            //为channel绑定所属的运行节点
            input.bindExecutionVertex(vertex);

            //输入管道和task绑定
            reduceTask.setInput(input);
            //每个task绑定一个输出池
            BufferPool<StreamElement> pool = new BufferPool<>();
            reduceBuffer.add(pool);
            reduceTask.setOutput(pool);
            //设置线程名称
            reduceTask.name("Reduce" + i);
            //放入管道集合
            reduceChannel.add(input);
            //放入运行实例集合
            reduceTaskList.add(reduceTask);
            environment.addTask(reduceTask);
        }

        //连接reduce和sink算子
        //为sink算子创建识别到checkpoint后保存数据的容器
        List<SinkBufferPool> result = new ArrayList<>();
        environment.setResult(result);
        SinkStreamTask<Tuple2<String, Integer>> sinkTask = new SinkStreamTask<>(result, reduceParrellism);
        //设置时间窗口大小
        sinkTask.setDuration(duration);
        //task命名
        sinkTask.name("Sink");
        //创建输入管道
        InputChannel<StreamElement> sinkInput = new InputChannel<>();
        sinkTask.setInput(sinkInput);
        environment.setSinkInput(sinkInput);
        //为sink的inputChannel绑定数据源buffer
        sinkInput.bindProviderBuffer(reduceBuffer);
        //为sink算子创建并绑定输出池
        BufferPool<StreamElement> sinkBufferPool = new BufferPool<>();
        sinkTask.setOutput(sinkBufferPool);
        environment.setSinkBuffer(sinkBufferPool);
        //为每一个上游buffer绑定输出到下游的channel
        for (int i = 0; i < reduceParrellism; i++) {
            reduceBuffer.get(i).bindInputChannel(Collections.singletonList(sinkInput));
        }

        //****创建全局运行环境
        //RunTimeEnvironment environment = new RunTimeEnvironment();
        //environment.addTasks(Collections.singletonList(consumer));
        //environment.addTasks(Collections.singletonList(sinkTask));
        //mapTaskList.forEach(environment::addTask);
        //reduceTaskList.forEach(environment::addTask);
        //所有task绑定全局运行环境
        consumer.setEnvironment(environment);
        sinkTask.setEnvironment(environment);
        for(StreamTask task : mapTaskList) task.setEnvironment(environment);
        for(StreamTask task:reduceTaskList) task.setEnvironment(environment);

        //****开始运行
        environment.start();
        //map算子
        for (int i = 0; i < mapParrellism; i++) {
            mapTaskList.get(i).start();
        }
        TimeUnit.SECONDS.sleep(1);
        //source算子
        consumer.start();
        TimeUnit.SECONDS.sleep(1);
        //reduce算子
        for (int i = 0; i < reduceParrellism; i++) {
            reduceTaskList.get(i).start();
        }
        TimeUnit.SECONDS.sleep(1);
        //kafka生产者
        KafkaSender sender = new KafkaSender();
        sender.start();
        //sink算子
        sinkTask.start();

        TimeUnit.SECONDS.sleep(60);

        System.out.println("【WordCount】 result: ");
        for (int i = 0; i < reduceParrellism; i++) {
            System.out.println("Reduce"+i);
            Collection<Tuple2<String, Integer>> res = reducerList.get(i).getValueState().get();
            for(Tuple2 tuple2 : res){
                System.out.println(tuple2);
            }
        }
    }
}
