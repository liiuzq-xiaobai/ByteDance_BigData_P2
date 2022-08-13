package common;

import function.KeySelector;
import record.StreamRecord;

/**
 * @author kevin.zeng
 * @description 将KeySelector<T,String>转换为KeySelector<StreamRecord<T>,String>
 * @create 2022-08-13
 */
public class KeySelectorWrapper<T> {

    public KeySelector<StreamRecord<T>,String> convert(KeySelector<T,String> keySelector) {
        return value -> keySelector.getKey(value.getValue());
    }
}
