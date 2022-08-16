package customized;

import function.KeySelector;
import record.Tuple2;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-16
 */
public class WordKeySelector implements KeySelector<Tuple2<String,Integer>,String> {

    @Override
    public String getKey(Tuple2<String, Integer> value) {
        return value.f0;
    }
}