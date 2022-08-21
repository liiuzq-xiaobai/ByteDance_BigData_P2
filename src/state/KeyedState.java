package state;


import java.util.Collection;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public interface KeyedState<K,V> {
    //根据key值取value
    V value(K key);

    V value();

    void update(V value);

    Collection<V> get();

    //清空状态中存储的数据
    void clear();
}
