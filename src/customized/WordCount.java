package customized;

import environment.StreamExecutionEnvironment;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-16
 */
public class WordCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.execute();
    }
}
