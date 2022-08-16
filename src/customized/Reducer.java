package customized;

import function.ReduceFunction;
import record.Tuple2;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-16
 */
public class Reducer implements ReduceFunction<Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
        Tuple2<String, Integer> result = new Tuple2<>();
        result.f0 = value1.f0;
        result.f1 = value1.f1 + value2.f1;
        return result;
    }
}
