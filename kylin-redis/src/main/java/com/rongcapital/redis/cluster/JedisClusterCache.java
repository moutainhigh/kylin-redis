package com.rongcapital.redis.cluster;

import com.rongcapital.redis.util.SerializeUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

public class JedisClusterCache {

	private JedisCluster jedisCluster;

	
	public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    public void setJedisCluster(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

  
	/**
	 * 获取对象
	 * 
	 * @param key
	 * @return Object
	 */
	public <T> T get(String key) throws Exception{
		T t = null;	
		try {		
			t = (T) SerializeUtil.valueDeserialize(jedisCluster.get(SerializeUtil.keySerialize(key)));
		}catch(Exception e){
		    e.printStackTrace();
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
	
		try {			
		    jedisCluster.set(SerializeUtil.keySerialize(key), SerializeUtil.valueSerialize(t));
			if (outTime != 0) {
			    jedisCluster.expire(SerializeUtil.keySerialize(key), outTime);
			}
		}catch(Exception e){
		    e.printStackTrace();
		}
		return true;
	}

}
