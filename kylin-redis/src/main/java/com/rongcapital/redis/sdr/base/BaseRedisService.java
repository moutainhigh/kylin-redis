package com.rongcapital.redis.sdr.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.serializer.RedisSerializer;

public class BaseRedisService<K, V> extends RedisTemplateBase<K, V> {

    /**
     * del
     * 
     * @param k
     * @return
     */
    public boolean delete(K k) throws Exception {
        try {
            getRedisTemplate().delete(k);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * exists
     * 
     * @param key
     * @return
     */
    public boolean hasKey(K key) throws Exception {

        return getRedisTemplate().hasKey(key);

    }

    /**
     * expire
     * 
     * @param key
     * @param timeout
     * @param unit
     * @return
     */
    public Boolean expire(K key, long timeout, TimeUnit unit) throws Exception {
        return getRedisTemplate().expire(key, timeout, unit);
    }

    /**
     * 批量设置过期时间，只要一个设置失败，则返回 Discription:
     * 
     * @param keys
     * @param timeout
     * @param unit
     * @return
     * @throws Exception Boolean
     * @author Administrator
     * @since 2016年3月29日
     */
    public Boolean expire(K[] keys, long timeout, TimeUnit unit) throws Exception {
        for (K k : keys) {
            if (!getRedisTemplate().expire(k, timeout, unit)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 当key存在时，返回false,不存在则添加成功返回true
     * 
     * @param key
     * @param value
     * @return
     */
    public boolean setNx(final K key, final V value) throws Exception {
        boolean result = execute(new RedisCallback<Boolean>() {
            public Boolean doInRedis(RedisConnection connection) throws DataAccessException {

                byte[] k = getKeySerializer().serialize(key);
                byte[] v = getValueSerializer().serialize(value);
                if (k != null) {
                    return connection.setNX(k, v);
                }
                return false;
            }
        });
        return result;
    }

    /**
     * add value
     * 
     * @param key
     * @param value
     * @param expire
     * @return
     */
    public boolean set(final K key, final V value, final int expire) throws Exception {
        boolean result = execute(new RedisCallback<Boolean>() {
            public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                byte[] k = getKeySerializer().serialize(key);
                byte[] v = getValueSerializer().serialize(value);
                if (v != null) {
                    connection.set(k, v);
                    if (expire != -1) {
                        connection.expire(k, expire);
                    }
                    return true;
                }
                return false;
            }
        });
        return result;
    }

    /**
     * get value
     * 
     * @param key
     * @return
     */
    public V get(final K key) throws Exception {
        V v = execute(new RedisCallback<V>() {
            public V doInRedis(RedisConnection connection) throws DataAccessException {
                RedisSerializer<K> serializer = getKeySerializer();
                byte[] k = serializer.serialize(key);
                byte[] value = connection.get(k);
                return getValueSerializer().deserialize(value);
            }

        });
        return v;
    }

    /**
     * get bath get
     * 
     * @param keys
     * @return
     */
    public List<V> bathGet(final K[] keys) throws Exception {
        List<V> values = execute(new RedisCallback<List<V>>() {
            @Override
            public List<V> doInRedis(RedisConnection connection) throws DataAccessException {
                RedisSerializer<K> serializer = getKeySerializer();
                List<byte[]> bytes = new ArrayList<byte[]>();
                List<V> result = new ArrayList<V>();
                for (K key : keys) {
                    byte[] k = serializer.serialize(key);
                    bytes.add(k);
                }
                int size = bytes.size();

                byte[][] keyArray = new byte[size][];

                for (int i = 0; i < keyArray.length; i++) {
                    keyArray[i] = bytes.get(i);
                }
                List<byte[]> temp = connection.mGet(keyArray);
                for (byte[] bt : temp) {
                    V v = getValueSerializer().deserialize(bt);
                    if (v != null) {
                        result.add(v);
                    }
                }
                return result;

            }
        });
        return values;
    }

    /**
     * bath set
     * 
     * @param data
     * @param expire
     * @return
     */
    public int bathSet(final Map<K, V> data, final int expire) throws Exception {
        if (data == null) {
            return 0;
        }
        int result = execute(new RedisCallback<Integer>() {
            @Override
            public Integer doInRedis(RedisConnection connection) throws DataAccessException {
                int inc = 0;
                RedisSerializer<K> serializer = getKeySerializer();
                Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
                for (K k : data.keySet()) {
                    V v = data.get(k);
                    byte[] kbt = serializer.serialize(k);
                    byte[] vbt = getValueSerializer().serialize(v);
                    if (vbt != null) {
                        connection.set(kbt, vbt);
                        connection.expire(kbt, expire);
                        ++inc;
                    }
                }
                return inc;
            }
        });
        return result;
    }

}
