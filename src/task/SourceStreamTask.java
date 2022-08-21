package task;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import record.CheckPointBarrier;
import record.CheckPointRecord;
import record.StreamRecord;
import record.Watermark;
import utils.WriteCheckPointUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author kevin.zeng
 * @description source算子
 * @create 2022-08-12
 */
public class SourceStreamTask extends StreamTask<String, String> {
    static final String TOPIC = "zbw_test2";
    static final String GROUP = "zbw_test_group2";
    int counter=0;
    public SourceStreamTask(){
        super("SOURCE");
    }
    private KafkaConsumer consumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new ConcurrentHashMap<>();//用于跟踪偏移量的map
    private TopicPartition topicPartition;
    //由runtimeenv调用的发起checkpoint请求函数
    public boolean sendcheckpointBarrier(){
        //得到当前的offset
        try {
            long offset = currentOffsets.get(topicPartition).offset();
            CheckPointRecord sourceckpoint = new CheckPointRecord("Source", this.getName(), offset, this.getState().toString());
            WriteCheckPointUtils.writeCheckPointFile("checkpoint/source.txt", sourceckpoint);
            return true;
        }catch (Exception e){
            System.out.println("sourece sendcheckpointBarrier called by runenv is wrong");
            e.printStackTrace();
            return true;
        }
    }
    private boolean nowSnapShot = false;

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
        consumer = new KafkaConsumer<>(props);
        try{
            consumer.subscribe(Collections.singletonList(TOPIC));
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record:records) {
                    counter++;
                    String obj = record.value();
                    StreamRecord<String> streamRecord = new StreamRecord<>(obj);
                    System.out.println(name + " produce: " + obj);
                    //放入下游的Buffer中，并将数据推向下游算子的输入管道
                    output.push(streamRecord);
                    //更新offset位置
                    topicPartition = new TopicPartition(record.topic(), record.partition());
                    currentOffsets.put(topicPartition, new OffsetAndMetadata(record.offset() + 1,"no matadata"));
                    //watermark每两条发一次
                    if(counter % 2 == 0){
                        Watermark watermark = new Watermark();
                        output.push(watermark);
                    }
                    TimeUnit.SECONDS.sleep(1);
                    if(counter % 4 == 0){
                        //保存offset，提交consumer的消费记录
                        CheckPointRecord sourceckpoint = new CheckPointRecord("Source",this.getName(),record.offset(),this.getState().toString());
                        WriteCheckPointUtils.writeCheckPointFile("checkpoint/source.txt",sourceckpoint);
                        //Kafka提交当前Offset
                        consumer.commitAsync(currentOffsets, null);
                        //下发checkpoint
                        CheckPointBarrier barrier = new CheckPointBarrier();
                        output.push(barrier);
                    }
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }

    }


    public static void main(String args[]) {
        SourceStreamTask receiver = new SourceStreamTask();
        receiver.start();
    }
}
