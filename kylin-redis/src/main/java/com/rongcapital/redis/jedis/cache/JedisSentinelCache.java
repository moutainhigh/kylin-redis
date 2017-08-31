package com.rongcapital.redis.jedis.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.rongcapital.redis.util.SerializeUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

/**
 * jedis主从操作
 * 
 * @author Administrator
 *
 */
public class JedisSentinelCache {

	private JedisSentinelPool jedisSentinelPool;


	public void setJedisSentinelPool(JedisSentinelPool jedisSentinelPool) {
		this.jedisSentinelPool = jedisSentinelPool;
	}

	public Jedis getJedis() {
		return jedisSentinelPool.getResource();
	}

	
	private void returnResource(Jedis jedis) {
		if (null != jedis)
			jedis.close();
	}
	/*---------------------------------string------------------------------*/

	public <T> boolean set(String key, T t) throws Exception{
		boolean flag = true;
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.set(SerializeUtil.keySerialize(key), SerializeUtil.valueSerialize(t));
		} finally {
			returnResource(jedis);
		}
		return flag;
	}

	public <T> boolean set(String key, T t, int outTime) throws Exception{
		boolean flag = true;
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.set(SerializeUtil.keySerialize(key), SerializeUtil.valueSerialize(t));
			if (outTime != 0) {
				jedis.expire(SerializeUtil.keySerialize(key), outTime);
			}		
		} finally {
			returnResource(jedis);
		}
		return flag;
	}

	/**
	 * 获取对象
	 * 
	 * @param key
	 * @return Object
	 */
	public <T> T get(String key) throws Exception{
		T t = null;
		Jedis jedis = null;
		try {
			jedis = getJedis();
			t = (T) SerializeUtil.valueDeserialize(jedis.get(SerializeUtil.keySerialize(key)));
		} finally {
			returnResource(jedis);
		}
		return t;
	}

	/**
	 * 设置一个key的过期时间（单位：秒）
	 * 
	 * @param key
	 *            key值
	 * @param seconds
	 *            多少秒后过期
	 * @return 1：设置了过期时间 0：没有设置过期时间/不能设置过期时间
	 */
	public long expire(String key, int seconds) throws Exception{
		if (key == null || key.equals("")) {
			return 0;
		}
		Jedis jedis = null;
		try {
			jedis = getJedis();
			return jedis.expire(SerializeUtil.keySerialize(key), seconds);
		} finally {
			returnResource(jedis);
		}
	}

	/**
	 * 设置一个key在某个时间点过期
	 * 
	 * @param key
	 *            key值
	 * @param unixTimestamp
	 *            unix时间戳，从1970-01-01 00:00:00开始到现在的秒数
	 * @return 1：设置了过期时间 0：没有设置过期时间/不能设置过期时间
	 */
	public long expireAt(String key, int unixTimestamp) throws Exception{
		if (key == null || key.equals("")) {
			return 0;
		}
		Jedis jedis = null;
		try {
			jedis = getJedis();
			return jedis.expireAt(SerializeUtil.keySerialize(key), unixTimestamp);
		} finally {
			returnResource(jedis);
		}
	}

	/**
	 * 删除对象
	 * 
	 * @param key
	 * @return Long
	 */
	public Long del(String key) throws Exception{
		Jedis jedis = null;
		try {
			jedis = getJedis();
			return jedis.del(SerializeUtil.keySerialize(key));
		} finally {
			returnResource(jedis);
		}
	}

	/**
	 * 判断对象是否存在
	 * 
	 * @param key
	 * @return true or false
	 */
	public boolean isExist(String key) throws Exception{
		Jedis jedis = null;
		try {
			jedis = getJedis();
			return jedis.exists(SerializeUtil.keySerialize(key));	
		} finally {
			returnResource(jedis);
		}
	
	}


}
