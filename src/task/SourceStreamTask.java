package task;

import main.DataStream;
import operator.StreamOperator;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import record.CheckPointBarrier;
import record.CheckPointRecord;
import record.StreamRecord;
import record.Watermark;
import utils.WriteCheckPointUtils;

import java.sql.Time;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author kevin.zeng
 * @description source算子
 * @create 2022-08-12
 */
public class SourceStreamTask extends StreamTask<String, String> {
    static final String TOPIC = "test";
    static final String GROUP = "test_group3";
    public SourceStreamTask(){
        super();
    }
    public boolean nowSnapshot = false;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();//用于跟踪偏移量的map
    public synchronized boolean sendCheckPointBarrier(){
        try{
            nowSnapshot = true;
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }
    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        //1.创建消费者配置类
        Properties props = new Properties();

        //2.给配置信息赋值
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"120.26.142.199:9092");

        //开启offset自动提交，不提交会重复消费
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        //向kafka提交offset的延迟
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");

        //key,value的反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //消费者组
		props.put(ConsumerConfig.GROUP_ID_CONFIG,GROUP);
        System.out.println("Receive message");

        try(KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);){
            consumer.subscribe(Collections.singletonList(TOPIC));

            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record:records) {
                    //runenv对sourcetask采取异步通信方式，runenv只是将sourecetask中的nowSnapshot标志位进行改变，不会等待sourcetask真正执行完具体逻辑后返回结果
                    if(nowSnapshot == true){
                        currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1,"no matadata"));
                        //把当前offset等其他metadata写入checkpoint
                        CheckPointRecord sourceckpoint = new CheckPointRecord("Source",this.getName(),record.offset(),this.getState().toString());
                        WriteCheckPointUtils.writeCheckPointFile("checkpoint/source.txt",sourceckpoint);
                        //Kafka提交当前Offset
                        consumer.commitAsync(currentOffsets, null);
                        nowSnapshot = false;
                        //往下游发送checkPointBarrier
                        CheckPointBarrier checkPointBarrier = new CheckPointBarrier();
                        output.push(checkPointBarrier);
                    }else{
                        String obj = record.value();
                        //每隔1s向下游传递一条数据
                        TimeUnit.MILLISECONDS.sleep(1000);
                        StreamRecord<String> streamRecord = new StreamRecord<>(obj);
                        //放入下游的Buffer中，并将数据推向下游算子的输入管道
                        output.push(streamRecord);
                        Watermark watermark = new Watermark();
                        output.push(watermark);
                        System.out.println(name + " produce: " + obj);
                    }
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }

    }


    public static void main(String args[]) throws InterruptedException {
        SourceStreamTask receiver = new SourceStreamTask();
        receiver.start();
    }
}
