package com.rongcapital.redis.sdr.manager;

import org.springframework.data.redis.core.RedisTemplate;


public class RedisTemplateManager<K, V> {

	private RedisTemplate<K, V> redisTemplate;

	public RedisTemplate<K, V> getRedisTemplate() {
		return redisTemplate;
	}

	public void setRedisTemplate(RedisTemplate<K, V> redisTemplate) {	
		this.redisTemplate = redisTemplate;
	}
	
}
