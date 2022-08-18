package common;


import java.util.Collection;
import java.util.List;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public interface KeyedState<K,V> extends ValueState<V> {
    //根据key值取value
    V value(K key);

    V value();

    void update(V value);

    Collection<V> get();

}
