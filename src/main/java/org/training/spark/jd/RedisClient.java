package org.training.spark.jd;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by banny on 2018/3/27.
 */
public class RedisClient {

    public static JedisPool jedisPool; // 池化管理jedis链接池

    static {

        //相关的配置
        String ip = "localhost";
        int port = Integer.parseInt("6379");
        int maxActive = 1024;
        int maxIdle = 200;
        int maxWait = 10000; //毫秒

        JedisPoolConfig config = new JedisPoolConfig();
        //设置最大连接数
        config.setMaxTotal(maxActive);
        //设置最大空闲数
        config.setMaxIdle(maxIdle);
        //设置超时时间
        config.setMaxWaitMillis(maxWait);
        //初始化连接池
        jedisPool = new JedisPool(config, ip, port);
    }

    /**
     * 向缓存中设置字符串内容
     *
     * @param key   key
     * @param value value
     * @return
     * @throws Exception
     */
    public static boolean set(String type,String key, Long value) throws Exception {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            //jedis.hset(type, key, value);
            jedis.hincrBy(type, key, value);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

}
