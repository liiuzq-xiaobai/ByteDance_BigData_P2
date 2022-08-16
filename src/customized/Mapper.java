package customized;

import function.MapFunction;
import record.Tuple2;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-16
 */
public class Mapper implements MapFunction<String, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> map(String value) {
        Tuple2<String, Integer> result = new Tuple2<>();
        result.f0 = value;
        result.f1 = 1;
        return result;
    }
}
