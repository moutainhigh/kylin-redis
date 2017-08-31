package com.rongcapital.redis.sdr.base;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

import com.rongcapital.redis.sdr.manager.RedisTemplateManager;

@SuppressWarnings("unchecked")
public class RedisTemplateBase<K, V> {
	
	
	@Autowired
	private RedisTemplateManager<K, V> redisTemplateManager;

	public RedisTemplate<K, V> getRedisTemplate() {
		return redisTemplateManager.getRedisTemplate();
	}

	protected <T> T execute(RedisCallback<T> callback) {
		return getRedisTemplate().execute(callback);
	}

	public RedisSerializer<K> getKeySerializer() {
		return (RedisSerializer<K>) getRedisTemplate().getKeySerializer();
	}

	public RedisSerializer<V> getValueSerializer() {
		return (RedisSerializer<V>) getRedisTemplate().getValueSerializer();
	}
}
