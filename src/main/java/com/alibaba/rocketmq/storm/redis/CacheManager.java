//package com.alibaba.rocketmq.storm.redis;
//
//import org.apache.commons.lang.math.NumberUtils;
//import org.apache.commons.lang.time.DateFormatUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;
//import redis.clients.jedis.Transaction;
//import redis.clients.jedis.exceptions.JedisConnectionException;
//
//import java.util.*;
//
///**
// * <p>
// * Wrapper of {@link Jedis} client.
// * </p>
// *
// * @author Xu Tao
// * @version 1.0
// * @since 1.0
// */
//public class CacheManager {
//
//
//    private static JedisPool pool;
//
//    static {
//        ResourceBundle bundle = ResourceBundle.getBundle("redis");
//        if (bundle == null)
//            throw new IllegalArgumentException("[redis.properties] is not found");
//
//        JedisPoolConfig config = new JedisPoolConfig();
//        //        config.setMaxActive(Integer.valueOf(bundle.getString("redis.pool.maxActive")));
//        config.setMaxIdle(Integer.valueOf(bundle.getString("redis.pool.maxIdle")));
//        config.setMaxWaitMillis(Long.valueOf(bundle.getString("redis.pool.maxWait")));
//        config.setTestOnBorrow(Boolean.valueOf(bundle.getString("redis.pool.testOnBorrow")));
//        config.setTestOnReturn(Boolean.valueOf(bundle.getString("redis.pool.testOnReturn")));
//        config.setMaxTotal(Integer.valueOf(bundle.getString("redis.pool.maxTotal")));
//        config.setMinIdle(Integer.valueOf(bundle.getString("redis.pool.minIdle")));
//        config.setTestOnBorrow(true);
//        pool = new JedisPool(config, bundle.getString("redis.ip"), Integer.valueOf(bundle.getString("redis.port")), 120);
//    }
//
//    private static CacheManager cacheManager = new CacheManager();
//
//    private static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);
//
//    private CacheManager() {
//    }
//
//    public static CacheManager getInstance() {
//        return cacheManager;
//    }
//
//    public void set(Map<String, String> entries) {
//        for ( Map.Entry<String, String> entry : entries.entrySet() ) {
//            pool.getResource().set(entry.getKey(), entry.getValue());
//        }
//    }
//
//    public void set(String key, String value) {
//        pool.getResource().set(key, value);
//    }
//
//    public boolean setKeyLive(Map<String, String> entries, int live) {
//        Transaction tx = pool.getResource().multi();
//        for ( Map.Entry<String, String> entry : entries.entrySet() ) {
//            tx.setex(entry.getKey(), live, entry.getValue());
//        }
//        List<Object> result = tx.exec();
//        if ( null == result || result.isEmpty() ) {
//            LOG.error("Failed to insert " + entries);
//            return false;
//        }
//
//        return true;
//    }
//
//    /**
//     * Set the value to the key and specify the key's life cycle in seconds.
//     *
//     * @param key
//     * @param live  Time to live in seconds.
//     * @param value
//     */
//    public void setKeyLive(String key, int live, String value) {
//        pool.getResource().setex(key, live, value);
//    }
//
//    public void publish(Map<String, String> entries, String channel) {
//        try {
//            Transaction tx = pool.getResource().multi();
//            for ( Map.Entry<String, String> entry : entries.entrySet() ) {
//                String key = entry.getKey();
//                tx.publish(channel, key);
//            }
//            tx.exec();
//        } catch ( Exception e ) {
//            LOG.error("Failed to publish.");
//        }
//    }
//
//    /**
//     * Append the value to an existing key
//     *
//     * @param key
//     * @param value
//     */
//    public void append(String key, String value) {
//        pool.getResource().append(key, value);
//    }
//
//    public String getValue(String key) {
//        return pool.getResource().get(key);
//    }
//
//    public List<String> getValues(String... keys) {
//        return pool.getResource().mget(keys);
//    }
//
//    public Set<String> getKeys(String pattern) {
//        return pool.getResource().keys(pattern);
//    }
//
//    public Long deleteValue(String key) {
//        return pool.getResource().del(key);
//    }
//
//    public Long deleteValues(String... keys) {
//        return pool.getResource().del(keys);
//    }
//
//    public void returnSource() {
//        RedisPoolManager.returnResource(pool.getResource());
//    }
//
//    public long calculateSize() {
//        return pool.getResource().dbSize();
//    }
//
//    public Long zadd(String key, double score, String value) {
//        Long result = pool.getResource().zadd(key, score, value);
//        expireAt(key, secondFromNextWeekZero());
//        return result;
//    }
//
//    public Long sadd(String key, String... value) {
//        LOG.info("jedis=" + pool.getResource() + ",key=" + key + ",value=" + value);
//        Long result = 0L;
//        try {
//            result = pool.getResource().sadd(key, value);
//            expireAt(key, secondFromNextWeekZero());
//        } catch ( JedisConnectionException e ) {
//            LOG.error("redis.sadd error:key=" + key + ",value=" + value, e);
//            if ( null != pool.getResource() ) {
//                pool.returnBrokenResource(jedis);
//            }
//        } finally {
//            if ( null != pool.getResource() )
//                pool.returnResource(pool.getResource());
//        }
//        return result;
//    }
//
//    public Double zscore(String key, String value) {
//        try {
//            return jedis.zscore(key, value);
//        } catch ( Exception e ) {
//            LOG.error("redis.zscore error:key=" + key + ",value=" + value, e);
//        }
//        return 0.0D;
//    }
//
//    public Long zaddAndIncScore(String key, double score, String value) {
//        try {
//            Double existsScore = zscore(key, value);
//            if ( null == existsScore ) {
//                existsScore = 0D;
//            }
//            score += existsScore.doubleValue();
//            return zadd(key, score, value);
//        } catch ( Exception e ) {
//            LOG.error("redis.zaddAndIncScore error:key=" + key + ",value=" + value + ",score=" + score, e);
//        }
//        return null;
//    }
//
//    public Long expireAt(String key, int unixTime) {
//        return jedis.expireAt(key, unixTime);
//    }
//
//    /** 下周零点的时间戳 */
//    public static int secondFromNextWeekZero() {
//        Calendar cal = Calendar.getInstance();
//        cal.add(Calendar.DAY_OF_YEAR, 7);
//        cal.set(Calendar.HOUR_OF_DAY, 0);
//        cal.set(Calendar.MINUTE, 0);
//        cal.set(Calendar.SECOND, 0);
//        return Integer.valueOf(cal.getTimeInMillis() / 1000 + "");
//    }
//
//    public static String KEY_OFFS_CONV_COUNT_ONDAY = "offs_conv_count_%s";
//
//    public static String keyOffsConvCount() {
//        return String.format(KEY_OFFS_CONV_COUNT_ONDAY, DateFormatUtils.format(Calendar.getInstance(), "yyyyMMddHH"));
//    }
//
//    public static String KEY_OFFS_CLIK_COUNT_ONDAY = "offs_clik_count_%s";
//
//    public static String keyOffsClikCount() {
//        return String.format(KEY_OFFS_CLIK_COUNT_ONDAY, DateFormatUtils.format(Calendar.getInstance(), "yyyyMMddHH"));
//    }
//
//    public static String KEY_PRE_AFFS_IN_OFF_ONDAY = "affs_in_offer_%s_%s";
//
//    public static String keyAffsInOff(String offId) {
//        return String.format(KEY_PRE_AFFS_IN_OFF_ONDAY, offId, DateFormatUtils.format(Calendar.getInstance(), "yyyyMMdd"));
//    }
//
//
//    public static void main(String[] args) {
//        System.out.println(keyOffsConvCount());
//        System.out.println(keyOffsClikCount());
//        System.out.println(keyAffsInOff("111"));
//        System.out.println(secondFromNextWeekZero());
//    }
//
//}
