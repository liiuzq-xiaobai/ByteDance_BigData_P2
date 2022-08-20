package test;

import common.KeySelectorWrapper;
import common.MapKeyedState;
import function.KeySelector;
import function.MapFunction;
import function.ReduceFunction;
import io.BufferPool;
import io.InputChannel;
import io.SinkBufferPool;
import main.KafkaSender;
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
public class TestPartition {
    private static final int DEFAULT_PARRELLISM = Runtime.getRuntime().availableProcessors();
    //用户自定义Function类存放的包
    private static final String CLIENT_CLASS_PATH = "customized.";
    //用户Properties文件路径
    private static final String CLIENT_FILE_PATH = "src" + File.separator + "customized" + File.separator + "config.properties";
    //TODO 相当于execute中的内容
    public static void main(String[] args) throws Exception {

        //****读取配置文件
        Properties props = new Properties();
        props.load(new FileReader(CLIENT_FILE_PATH));
        int mapParrellism = props.get("map.parrellism") == null ? DEFAULT_PARRELLISM : Integer.valueOf(String.valueOf(props.get("map.parrellism")));
        int reduceParrellism = props.get("reduce.parrellism") == null ? DEFAULT_PARRELLISM : Integer.valueOf(String.valueOf(props.get("reduce.parrellism")));
        String keySelectorClassName = String.valueOf(props.get("keySelector.class"));
        String mapFunctionClassName = String.valueOf(props.get("mapFunction.class"));
        String reduceFunctionClassName = String.valueOf(props.get("reduceFunction.class"));

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

        //TODO 以下为DAG图构造过程（此处只用了硬代码）

        /*TODO 为处理Watermark等多种类型数据，
           BufferPool，InputChannel等数据结构存放类型可能要修改为数据基类StreamElement
         */

        //****连接source和map算子
        //1个source 2个map
        SourceStreamTask consumer = new SourceStreamTask();
        consumer.name("Source");

        //map算子的运行节点
        ExecutionJobVertex<String, Tuple2<String, Integer>> vertex = new ExecutionJobVertex<>();
        //创建map算子，声明key选择器
        OneInputStreamOperator<String, Tuple2<String, Integer>, MapFunction<String, Tuple2<String, Integer>>> mapper = new StreamMap<>(mapFunction);
        //运行节点中包含的运行实例task
        List<StreamTask<String, Tuple2<String, Integer>>> mapTaskList = new ArrayList<>();
        vertex.setTasks(mapTaskList);
        //source算子绑定下游输出Buffer
        BufferPool<StreamElement> sourceBuffer = new BufferPool<>();
        consumer.setOutput(sourceBuffer);
        //map算子的channel集合
        List<InputChannel<StreamElement>> mapChannel = new ArrayList<>();
        //channel输入集合要和上游task的buffer绑定，以实现并行
        sourceBuffer.bindInputChannel(mapChannel);
        //算子有几个并行度就会有几个bufferPool，输出数据类型为map
        List<BufferPool<StreamElement>> mapBuffer = new ArrayList<>();
        for (int i = 0; i < mapParrellism; i++) {
            BufferPool<StreamElement> pool = new BufferPool<>();
            //map算子的bufferPool要启用分区，以便进行reduce计算
            pool.enablePartition();
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
        }


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
        //reduce算子的运行节点
        ExecutionJobVertex<Tuple2<String, Integer>, Tuple2<String, Integer>> reduceVertex = new ExecutionJobVertex<>();
        //运行节点中包含的运行实例task
        List<StreamTask<Tuple2<String, Integer>, Tuple2<String, Integer>>> reduceTaskList = new ArrayList<>();
        reduceVertex.setTasks(reduceTaskList);
        //reduce算子的channel集合
        List<InputChannel<StreamElement>> reduceChannel = new ArrayList<>();
        for (int i = 0; i < mapParrellism; i++) {
            //map的每一个buffer都绑定reduce算子的所有input管道，按哈希值%管道数进行shuffle
            mapBuffer.get(i).bindInputChannel(reduceChannel);
        }
        //
        //reduce算子的buffer集合
        List<BufferPool<StreamElement>> reduceBuffer = new ArrayList<>();
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
        }


        //连接reduce和sink算子

        //为sink算子创建识别到checkpoint后保存数据的容器
        List<SinkBufferPool> result = new ArrayList<>();
        SinkStreamTask<Tuple2<String, Integer>> sinkTask = new SinkStreamTask<>(result, reduceParrellism);
        //为sink算子绑定下游输出Buffer
        BufferPool<StreamElement> sinkBuffer = new BufferPool<>();
        sinkTask.setOutput(sinkBuffer);
        //task命名
        sinkTask.name("Sink");
        //创建输入管道
        InputChannel<StreamElement> sinkInput = new InputChannel<>();
        sinkTask.setInput(sinkInput);
        //为sink的inputChannel绑定数据源buffer
        sinkInput.bindProviderBuffer(reduceBuffer);
        //为sink算子创建并绑定输出池
        BufferPool<StreamElement> sinkBufferPool = new BufferPool<>();
        sinkTask.setOutput(sinkBufferPool);


        /*TODO 当前数据传输方式为 Buffer push数据 到InputChannel
                是否需要改成InputChannel 从 上游Buffer拉取数据
         */
        //为每一个上游buffer绑定输出到下游的channel
        for (int i = 0; i < reduceParrellism; i++) {
            reduceBuffer.get(i).bindInputChannel(Collections.singletonList(sinkInput));
        }


        //****开始运行
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

        TimeUnit.SECONDS.sleep(90);

        for (int i = 0; i < reduceParrellism; i++) {
            System.out.println(Thread.currentThread().getName() + " 【WordCount】 result: " + reducerList.get(i).getValueState().get());
        }
    }
}
