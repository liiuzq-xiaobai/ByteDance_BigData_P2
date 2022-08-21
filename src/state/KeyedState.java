package state;


import java.util.Collection;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
//算子状态存储
public interface KeyedState<K,V> {
    //根据key值取value
    V value(K key);

    //传入value值更新状态
    void update(V value);

    //更新value值的集合
    Collection<V> get();

    //清除状态
    void clear();
}
