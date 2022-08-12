package task;

import main.DataStream;
import operator.StreamOperator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import record.StreamRecord;

import java.time.Duration;
import java.util.*;

/**
 * @author kevin.zeng
 * @description source算子
 * @create 2022-08-12
 */
public class SourceStreamTask extends StreamTask<String, String> {
    static final String TOPIC = "test";
    static final String GROUP = "test_group1";


    Random rand = new Random();
    /**
     public void setQueue(LinkedBlockingDeque<ObjectWrapper> sharedQueue) {
     this.dataQueue = sharedQueue;
     }
     */
    public SourceStreamTask(){
        super();
    }



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
                    String obj = record.value();
                    StreamRecord<String> streamRecord = new StreamRecord<>(obj);
                    //放入下游的Buffer中
                    output.add(streamRecord);
                    System.out.println(name + " produce: " + obj);
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
