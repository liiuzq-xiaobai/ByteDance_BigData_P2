package customized;

import environment.StreamExecutionEnvironment;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-16
 */
public class WordCount {
    public static void main(String[] args) {
        //用户创建流式计算运行环境，调用execute方法即可运行程序
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.execute();
    }
}
