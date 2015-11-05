package com.alibaba.rocketmq.storm.redis;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.*;

/**
 * Created by penuel on 14-7-16.
 */
public class RedisClient {

    private static Logger LOG = LoggerFactory.getLogger(RedisClient.class);

    public static final int SECOND = 1;

    public static final int MINUTE = 60 * SECOND;

    public static final int HOUR = 60 * MINUTE;

    public static final int DAY = 24 * HOUR;

    static JedisPool pool;

    static {
        ResourceBundle bundle = ResourceBundle.getBundle("redis");
        if ( bundle == null )
            throw new IllegalArgumentException("[redis.properties] is not found");

        JedisPoolConfig config = new JedisPoolConfig();
        //        config.setMaxActive(Integer.valueOf(bundle.getString("redis.pool.maxActive")));
        config.setMaxIdle(Integer.valueOf(bundle.getString("redis.pool.maxIdle")));
        config.setMaxWaitMillis(Long.valueOf(bundle.getString("redis.pool.maxWait")));
        config.setTestOnBorrow(Boolean.valueOf(bundle.getString("redis.pool.testOnBorrow")));
        config.setTestOnReturn(Boolean.valueOf(bundle.getString("redis.pool.testOnReturn")));
        config.setMaxTotal(Integer.valueOf(bundle.getString("redis.pool.maxTotal")));
        config.setMinIdle(Integer.valueOf(bundle.getString("redis.pool.minIdle")));
        LOG.warn("JedisConfig : " + JSONObject.toJSONString(config) + ",redis.ip=" + bundle.getString("redis.ip") + ",redis.port=" + bundle.getString(
                "redis.port"));
        pool = new JedisPool(config, bundle.getString("redis.ip"), Integer.valueOf(bundle.getString("redis.port")), 120);

    }

    private static RedisClient redisClient = new RedisClient();

    private RedisClient() {

    }

    public static RedisClient getInstance() {

        if ( null == redisClient ) {
            redisClient = new RedisClient();
        }
        return redisClient;
    }

    public String get(String key) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.get(key);
        } catch ( JedisConnectionException e ) {
            LOG.error("RedisClient Error:", e);
            if ( null != jedis ) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if ( null != jedis )
                jedis.close();
        }
        return null;
    }

    public void set(Map<String, String> entries) {
        for ( Map.Entry<String, String> entry : entries.entrySet() ) {
            set(entry.getKey(), entry.getValue(), null);
        }
    }

    public String set(String key, String value, Integer expire) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            String l = jedis.set(key, value);
            if ( expire != null ) {
                jedis.expire(key, expire);
            }
            return l;
        } catch ( JedisConnectionException e ) {
            LOG.error("RedisClient Error:", e);
            if ( null != jedis ) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if ( null != jedis )
                jedis.close();
        }
        return null;
    }

    public Long zadd(String key, double score, String value, int expire) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            Long result = jedis.zadd(key, score, value);
            if ( 0 != expire ) {
                jedis.expire(key, expire);
            }
            return result;
        } catch ( JedisConnectionException e ) {
            LOG.error("RedisClient Error:", e);
            if ( null != jedis ) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if ( null != jedis )
                jedis.close();
        }
        return 0L;
    }

    public Long zadd(String key, Map<String,Double> scoreValueMap, int expire) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            Long result = jedis.zadd(key,scoreValueMap);
            if ( 0 != expire ) {
                jedis.expire(key, expire);
            }
            return result;
        } catch ( JedisConnectionException e ) {
            LOG.error("RedisClient Error:", e);
            if ( null != jedis ) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if ( null != jedis )
                jedis.close();
        }
        return 0L;
    }

    public Double zscore(String key, String value) {

        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            Double l = jedis.zscore(key, value);
            return l;
        } catch ( JedisConnectionException e ) {
            LOG.error("RedisClient Error:", e);
            if ( null != jedis ) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if ( null != jedis )
                jedis.close();
        }
        return null;
    }

    public boolean setKeyLive(Map<String, String> entries, int live) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            Transaction tx = jedis.multi();
            for ( Map.Entry<String, String> entry : entries.entrySet() ) {
                tx.setex(entry.getKey(), live, entry.getValue());
            }
            List<Object> result = tx.exec();
            if ( null == result || result.isEmpty() ) {
                LOG.error("Failed to insert " + entries);
                return false;
            }
        } catch ( JedisConnectionException e ) {
            LOG.error("RedisClient Error:", e);
            if ( null != jedis ) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if ( null != jedis )
                jedis.close();
        }

        return true;
    }

    public void publish(Map<String, String> entries, String channel) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            Transaction tx = jedis.multi();
            for ( Map.Entry<String, String> entry : entries.entrySet() ) {
                String key = entry.getKey();
                tx.publish(channel, key);
            }
            tx.exec();
        } catch ( JedisConnectionException e ) {
            LOG.error("publish", e);
            if ( null != jedis ) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if ( null != jedis )
                jedis.close();
        }
    }

    public Long expireAt(String key, int unixTime) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.expireAt(key, unixTime);
        } catch ( JedisConnectionException e ) {
            LOG.error("expireAt", e);
            if ( null != jedis ) {
                pool.returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if ( null != jedis )
                jedis.close();
        }
        return null;
    }

    public void zaddByTimeStamp(String key, Set<String> values) {
        if ( values == null ){
            return;
        }
        Map<String,Double> scoreValues = new HashMap<>();
        for ( String value : values ) {
            scoreValues.put(value, (double)System.currentTimeMillis());
        }
        zadd(key,scoreValues, DAY * 30);
    }

}