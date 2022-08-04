# ByteDance_BigData_P2
字节跳动第四届青训营大数据实训项目2

## 1. Kafka数据发送和接收
### 1.1 Kafka set up
- 在Kafka安装目录下run：
	- bin/zookeeper-server-start.sh config/zookeeper.properties
	- bin/kafka-server-start.sh config/server.properties

- 创建topic和bootstrap server：
	- bin/kafka-topics.sh --create --topic **TOPIC** --bootstrap-server **localhost:9092**

### 1.2 数据发送和接受
- KafkaSender.java 可以实现Kafka消息的发送
- KafkaReceiver.java可以实现Kafka消息的接收