# ByteDance_BigData_P2 ｜ 字节跳动第四届青训营大数据实训项目2
branch author：郑之恺 kkevin26

## 1. Kafka数据发送和接收
### 1.1 Kafka set up
- 在Kafka安装目录下run：
	- bin/zookeeper-server-start.sh config/zookeeper.properties
	- bin/kafka-server-start.sh config/server.properties

- 创建topic和bootstrap server：
	- bin/kafka-topics.sh --create --topic **TOPIC** --bootstrap-server **localhost:9092**

### 1.2 实现功能
- KafkaSender.java 可以实现Kafka消息的发送
	- 目前hardcode为每隔2秒随机发送一个字母
- KafkaReceiver.java可以实现Kafka消息的接收，并且把收到的Kafka信息转化为一个Object存放在BlockingQueue中
- Mapper.java （当前是Mapper/Reducer/Sink的集合体，只有最简单功能）
	- run应修改为abstract handler形式，方便用户自定义
	- 目前hardcode为将收到的Object存放进一个HashMap中，并统计数量；每隔1分钟print当前HashMap中的数据
	- 需要考虑不同需求下如何实现，以及用户如何提交HashMap

### 1.3 待开发功能
- Mapper拆分为Mapper/Reducer/Sink
- 增加并行度，在多并行下如何shuffle分散数据
- 自定义Mapper/Reducer function API
- shuffle （当前为 单source->单mapper 无shuffle）
- 故障支持


### 1.4 运行方法
- 按照1.1完成Kafka设置
- mvn clean install exec:java 运行main