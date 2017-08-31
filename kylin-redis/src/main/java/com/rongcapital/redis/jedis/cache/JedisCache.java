package com.rongcapital.redis.jedis.cache;

import com.rongcapital.redis.util.SerializeUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisCache {

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
	 * 保存对象
	 * 
	 * @param key
	 * @param obj
	 * @param outTime
	 * @return
	 */
	public <T> boolean set(String key, T t, int outTime) throws Exception{
		Jedis jedis = null;
		boolean flag = true;
		try {
			jedis = getJedis();
			jedis.set(SerializeUtil.keySerialize(key), SerializeUtil.valueSerialize(t));
			if (outTime != 0) {
				getJedis().expire(SerializeUtil.keySerialize(key), outTime);
			}
		} finally {
			returnResource(jedis);

		}
		return flag;
	}

}
