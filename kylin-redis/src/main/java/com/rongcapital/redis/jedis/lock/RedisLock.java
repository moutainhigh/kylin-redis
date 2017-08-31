package com.rongcapital.redis.jedis.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;


/**
 * redis分布式锁
 * 
 * @author Administrator
 *
 */
public class RedisLock {
	
	
	private JedisPool jedisPool;

	public void setJedisPool(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

	public Jedis getJedis() {
		return jedisPool.getResource();
	}
	
	/**
	 * 释放连接
	 */
	public void returnResource(Jedis jedis) {
		if (null != jedis)
			jedis.close();
	}
	/**
	 * 取锁入口方法
	 * 
	 * @param key
	 *            业务key
	 * @param lockTime
	 *            锁定时间
	 * @param sleepTime
	 *            循环取停顿时间
	 * @param timeOut
	 *            循环取锁时间
	 * @param timeUnit
	 *            时间单位
	 * @return
	 */
	public boolean tryLock(String key, int lockTime, int sleepTime, long timeout, TimeUnit unit) throws Exception{
		Jedis jedis = null;
		try {
			jedis = getJedis();
			long nano = System.nanoTime(); // 秒
			/*--在timeout时间内循环获取锁--*/
			do {

				Long i = jedis.setnx(key, String.valueOf(System.currentTimeMillis() + lockTime));
				if (i == 1) {
					jedis.expire(key, lockTime);
					return Boolean.TRUE;
				} else {
					String entTime = jedis.get(key); // 获取当前键最大生成时间
					// 如果下边的情况，某个实例setnx成功后 crash
					// 导致紧跟着的expire没有被调用，这时可以直接设置expire并把锁纳为己用。
					if (null == entTime || "".equals(entTime)
                            || System.currentTimeMillis()>Long.parseLong(entTime)) {
						jedis.expire(key, lockTime); // 重新设置过期时间
						return Boolean.TRUE;
					}
				}
				Thread.sleep(sleepTime);
			} while ((System.nanoTime() - nano) < unit.toNanos(timeout));
			return Boolean.FALSE;
		} finally {
			jedis.close();
		}		
	}

	/**
	 * 批量取锁
	 * 如果全部获取成功，则返回true,否则只要有一个不成功，就返回false
	 * @param keys
	 * @param timeout
	 * @param unit
	 * @return
	 */
	public boolean tryLock(String[] keys, int lockTime, int sleepTime, long timeout, TimeUnit unit) throws Exception{
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			List<String> needLocking = new ArrayList<String>();
			List<String> locked = new ArrayList<String>();
			long nano = System.nanoTime();
			do {
				// 构建pipeline，批量提交
				Pipeline pipeline = jedis.pipelined();

				for (String key : keys) {
					needLocking.add(key);
					pipeline.setnx(key, key);
				}
				// 提交redis执行计数
				List<Object> results = pipeline.syncAndReturnAll();
				for (int i = 0; i < results.size(); ++i) {
					Long result = (Long) results.get(i); // 执行结果
					String key = needLocking.get(i);
					if (result == 1) { // setnx成功，获得锁
						jedis.expire(key, lockTime);
						locked.add(key);
					}else{
					    String entTime = jedis.get(key); // 获取当前键最大生成时间
	                    // 如果下边的情况，某个实例setnx成功后 crash
	                    // 导致紧跟着的expire没有被调用，这时可以直接设置expire并把锁纳为己用。
	                    if (null == entTime || "".equals(entTime)
	                            || System.currentTimeMillis()>Long.parseLong(entTime)) {
	                        jedis.expire(key, lockTime);
	                        locked.add(key);
	                    }
					}
				}

				needLocking.removeAll(locked); // 已锁定资源去除

				if (CollectionUtils.isEmpty(needLocking)) {
					return true;
				}

				if (timeout == 0) {
					break;
				}
				Thread.sleep(sleepTime);
			} while ((System.nanoTime() - nano) < unit.toNanos(timeout));

			// 得不到锁，释放锁定的部分对象，并返回失败
			if (!CollectionUtils.isEmpty(locked)) {
				for (String lock : locked) {
					jedis.del(lock);
				}

			}
			return false;
		} finally {
			returnResource(jedis);
		}
	
	}

	/**
	 * 释放锁
	 * 
	 * @param key
	 */
	public void unLock(String key) throws Exception{
		Jedis jedis=null;
		try {
			jedis=getJedis();
			jedis.del(key);
		} finally {
			returnResource(jedis);
		}
	}

	/**
	 * 批量释放锁
	 * 
	 * @param keys
	 */
	public void unLock(String[] keys) throws Exception{
		Jedis jedis=null;
		try {
			jedis=getJedis();
			if (null != keys && keys.length > 0) {
				for (String key : keys) {
					getJedis().del(key);
				}
			}
		} finally {
			returnResource(jedis);
		}
	}

}