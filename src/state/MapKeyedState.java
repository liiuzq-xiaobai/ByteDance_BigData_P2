package state;

import function.KeySelector;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public class MapKeyedState<K,V> implements KeyedState<K,V> {

    final KeySelector<V,K> keySelector;

    Map<K,V> map = new HashMap<>();

    public MapKeyedState(KeySelector<V, K> keySelector) {
        this.keySelector = keySelector;
    }

    //返回key值对应的value
    @Override
    public V value() {
        return null;
    }

    @Override
    public void update(V value) {
        K key = keySelector.getKey(value);
        map.put(key,value);
        System.out.println(Thread.currentThread().getName() + "【update KeyedState to】" + map.values());
    }

    @Override
    public V value(K key) {
        V value = map.get(key);
        return value;
    }

    @Override
    public Collection<V> get() {
        return map.values();
    }

    @Override
    public void clear() {
        map.clear();
    }
}
