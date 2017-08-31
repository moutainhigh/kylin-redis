package com.rongcapital.redis.sdr.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import com.rongcapital.redis.sdr.base.BaseRedisService;

/**
 * redis分布式锁
 * 
 * @author Administrator
 *
 */
public class RedisLock extends BaseRedisService<String, String> {
    private static Logger logger = LoggerFactory.getLogger(RedisLock.class);

    /**
     * 取锁入口方法
     * 
     * @param key 业务key
     * @param lockTime 锁定时间
     * @param sleepTime 循环取停顿时间
     * @param timeOut 循环取锁时间
     * @param timeUnit 时间单位
     * @return
     */
    public boolean tryLock(String key, long lockTime, long sleepTime, long timeout, TimeUnit unit) throws Exception {

        long nano = System.nanoTime();
        /*--在timeout时间内循环获取锁--*/
        do {
            String value = String.valueOf(System.currentTimeMillis() + unit.toNanos(lockTime));
            boolean isTure = setNx(key, value);
            if (isTure) {
                expire(key, lockTime, unit);
                return Boolean.TRUE;
            } else {
                String entTime = get(key); // 获取当前键最大生成时间
                // 如果下边的情况，某个实例setnx成功后 crash
                // 导致紧跟着的expire没有被调用，这时可以直接设置expire并把锁纳为己用。
                if (null == entTime || "".equals(entTime)
                        || System.currentTimeMillis()>Long.parseLong(entTime)) {
                    expire(key, lockTime, unit); // 重新设置过期时间
                    return Boolean.TRUE;
                }

            }
            Thread.sleep(sleepTime);
        } while ((System.nanoTime() - nano) < unit.toNanos(timeout));

        return Boolean.FALSE;
    }

    /**
     * 如果全部获取成功，则返回true,否则只要有一个不成功，就返回false
     * 
     * @param keys
     * @param timeout
     * @param unit
     * @return
     */
    public boolean tryLock(String[] keys, long lockTime, long sleepTime, long timeout, TimeUnit unit) throws Exception {

        if (null == keys || keys.length == 0) {
            throw new Exception("keys为null");
        }
        List<String> locked = new ArrayList<String>();
        long nano = System.nanoTime();
        do {
            String value = String.valueOf(System.currentTimeMillis() + unit.toNanos(lockTime));
            List<Object> results = pipeline(keys, value, lockTime);
            for (int i = 0; i < results.size(); ++i) {
                Boolean result = (Boolean) results.get(i); // 执行结果
                String key = keys[i];
                if (result) { // setnx成功，获得锁
                    expire(key, lockTime, unit);
                    locked.add(key);
                } else {
                    String entTime = get(key);
                    if (null == entTime || "".equals(entTime)
                            || System.currentTimeMillis()>Long.parseLong(entTime)) {
                        expire(key, lockTime, unit); // 重新设置过期时间
                        locked.add(key);
                    }
                }
            }

            if (locked.size() == keys.length) {
                return true;
            } else {
                for (int i = 0; i < locked.size(); i++) {
                    delete(locked.get(i));
                    locked.remove(i);
                }
            }

            if (timeout == 0) {
                break;
            }
            Thread.sleep(sleepTime);
        } while ((System.nanoTime() - nano) < unit.toNanos(timeout));
        return false;
    }

    /**
     * pipeline
     * 
     * @param keys
     */
    public List<Object> pipeline(final String[] keys, final String value, final long lockTime) throws Exception {
        RedisCallback<List<Object>> pipelineCallback = new RedisCallback<List<Object>>() {
            @Override
            public List<Object> doInRedis(RedisConnection connection) throws DataAccessException {
                connection.openPipeline();     
                for (String key : keys) {                 
                    connection.setNX(getKeySerializer().serialize(key), getValueSerializer().serialize(value));
                }
                return connection.closePipeline();
            }
        };
        List<Object> results = (List<Object>) getRedisTemplate().execute(pipelineCallback);
        return results;
    }

    /**
     * 释放锁
     * 
     * @param key
     */
    public void unLock(String key) throws Exception {
        delete(key);
    }

    /**
     * 批量释放锁
     * 
     * @param keys
     */
    public void unLock(String[] keys) throws Exception {
        if (null != keys && keys.length > 0) {
            for (String key : keys) {
                delete(key);
            }
        }
    }

}