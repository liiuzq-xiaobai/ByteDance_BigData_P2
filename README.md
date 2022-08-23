# ByteDance_BigData_P2 ｜ 字节跳动第四届青训营大数据实训项目2
### 操作指南

1. 从github链接https://github.com/liiuzq-xiaobai/ByteDance_BigData_P2上拉取项目后，进入src/customized子包。

2. 在customized包下，定义MapFunction, ReduceFunction, KeySelector接口的实现类（类名可自定义），样例分别为

   - MapFunction

   ```java
   public class Mapper implements MapFunction<String, Tuple2<String, Integer>> {
       @Override
       public Tuple2<String, Integer> map(String value) {
           //...
       }
   }
   ```

   - ReduceFunction

   ```java
   public class Reducer implements ReduceFunction<Tuple2<String, Integer>> {
       @Override
       public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
           //...
       }
   }
   ```

   - KeySelector

   ```java
   public class WordKeySelector implements KeySelector<Tuple2<String,Integer>,String> {
       @Override
       public String getKey(Tuple2<String, Integer> value) {
   		//...
       }
   }
   ```


3. 在config.properties文件下，定义算子的并行度、自定义函数的类名、窗口大小（单位为秒）等信息。样例为

```properties
#map算子并行度
map.parrellism=2
#reduce算子并行度
reduce.parrellism=2
#用户定义的KeySelector类名
keySelector.class=WordKeySelector
#用户定义的MapFunction类名
mapFunction.class=Mapper
#用户定义的ReduceFunction类名
reduceFunction.class=Reducer
#窗口大小，单位为s
window.size=20
```

4. 好啦，准备工作完成！在customized包下，创建属于你自己的运行主类，然后在main方法中创建流式计算的运行环境StreamExecutionEnvironment，调用运行的环境的execute方法，即可运行wordcount程序！样例为

```java
public class WordCount {
    public static void main(String[] args) {
        //创建流式计算运行环境，调用execute方法即可运行程序
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.execute();
    }
}
```



