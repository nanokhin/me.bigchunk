package me.bigchunk.ex.utils;

import java.util.Map;

/**
 * vladvlaskin | 8/29/12/7:54 PM
 */
public class MeEntry<K, V> implements Map.Entry<K, V> {

    private final K key;
    private V value;

    public MeEntry(K key, V value) {
        Object o = new Object();

        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V v) {
        V old = value;
        value = v;
        return old;
    }
}
