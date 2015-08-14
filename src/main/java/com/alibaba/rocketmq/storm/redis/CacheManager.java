package com.alibaba.rocketmq.storm.redis;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;

/**
 * <p>
 * Wrapper of {@link Jedis} client.
 * </p>
 *
 * @author Xu Tao
 * @version 1.0
 * @since 1.0
 */
public class CacheManager {

    private Jedis jedis = RedisPoolManager.createInstance();

    private static CacheManager cacheManager = new CacheManager();

    private static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);

    private CacheManager() {
    }

    public static CacheManager getInstance() {
        return cacheManager;
    }

    public void set(Map<String, String> entries) {
        for ( Map.Entry<String, String> entry : entries.entrySet() ) {
            jedis.set(entry.getKey(), entry.getValue());
        }
    }

    public void set(String key, String value) {
        jedis.set(key, value);
    }

    public boolean setKeyLive(Map<String, String> entries, int live) {
        Transaction tx = jedis.multi();
        for ( Map.Entry<String, String> entry : entries.entrySet() ) {
            tx.setex(entry.getKey(), live, entry.getValue());
        }
        List<Object> result = tx.exec();
        if ( null == result || result.isEmpty() ) {
            LOG.error("Failed to insert " + entries);
            return false;
        }

        return true;
    }

    /**
     * Set the value to the key and specify the key's life cycle in seconds.
     *
     * @param key
     * @param live  Time to live in seconds.
     * @param value
     */
    public void setKeyLive(String key, int live, String value) {
        jedis.setex(key, live, value);
    }

    public void publish(Map<String, String> entries, String channel) {
        try {
            Transaction tx = jedis.multi();
            for ( Map.Entry<String, String> entry : entries.entrySet() ) {
                String key = entry.getKey();
                tx.publish(channel, key);
            }
            tx.exec();
        } catch ( Exception e ) {
            LOG.error("Failed to publish.");
        }
    }

    /**
     * Append the value to an existing key
     *
     * @param key
     * @param value
     */
    public void append(String key, String value) {
        jedis.append(key, value);
    }

    public String getValue(String key) {
        return jedis.get(key);
    }

    public List<String> getValues(String... keys) {
        return jedis.mget(keys);
    }

    public Set<String> getKeys(String pattern) {
        return jedis.keys(pattern);
    }

    public Long deleteValue(String key) {
        return jedis.del(key);
    }

    public Long deleteValues(String... keys) {
        return jedis.del(keys);
    }

    public void returnSource() {
        RedisPoolManager.returnResource(jedis);
    }

    public long calculateSize() {
        return jedis.dbSize();
    }

    public Long zadd(String key, double score, String value) {
        Long result = jedis.zadd(key, score, value);
        expireAt(key, secondFromNextWeekZero());
        return result;
    }

    public Long sadd(String key, String... value) {
        LOG.info("jedis="+jedis+",key="+key+",value="+value);
        Long result = jedis.sadd(key, value);
        expireAt(key, secondFromNextWeekZero());
        return result;
    }

    public Double zscore(String key, String value) {
        return jedis.zscore(key, value);
    }

    public Long zaddAndIncScore(String key, double score, String value) {
        Double existsScore = zscore(key, value);
        if ( null == existsScore ) {
            existsScore = 0D;
        }
        score += existsScore.doubleValue();
        return zadd(key, score, value);

    }

    public Long expireAt(String key, int unixTime) {
        return jedis.expireAt(key, unixTime);
    }

    /** 下周零点的时间戳 */
    public static int secondFromNextWeekZero() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, 7);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return Integer.valueOf(cal.getTimeInMillis() / 1000+"");
    }

    public static String KEY_OFFS_CONV_COUNT_ONDAY = "offs_conv_count_%s";

    public static String keyOffsConvCount() {
        return String.format(KEY_OFFS_CONV_COUNT_ONDAY, DateFormatUtils.format(Calendar.getInstance(), "yyyyMMddHH"));
    }

    public static String KEY_OFFS_CLIK_COUNT_ONDAY = "offs_clik_count_%s";

    public static String keyOffsClikCount() {
        return String.format(KEY_OFFS_CLIK_COUNT_ONDAY, DateFormatUtils.format(Calendar.getInstance(), "yyyyMMddHH"));
    }

    public static String KEY_PRE_AFFS_IN_OFF_ONDAY = "affs_in_offer_%s_%s";

    public static String keyAffsInOff(String offId) {
        return String.format(KEY_PRE_AFFS_IN_OFF_ONDAY, offId, DateFormatUtils.format(Calendar.getInstance(), "yyyyMMdd"));
    }

    public static void main(String[] args) {
        System.out.println(keyOffsConvCount());
        System.out.println(keyOffsClikCount());
        System.out.println(keyAffsInOff("111"));
        System.out.println(secondFromNextWeekZero());
    }

}
