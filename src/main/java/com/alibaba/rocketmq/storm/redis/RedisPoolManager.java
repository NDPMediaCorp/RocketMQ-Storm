//package com.alibaba.rocketmq.storm.redis;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;
//
//import java.util.ResourceBundle;
//
///**
// * <p>
// *     Redis pool manager to better handle life cycle of {@link Jedis} client instances.
// * </p>
// *
// * @author Xu tao
// * @version 1.0
// * @since 1.0
// */
//public class RedisPoolManager {
//
//
//    private static final Logger LOG = LoggerFactory.getLogger(RedisPoolManager.class);
//
//    private static JedisPool pool;
//
//    static {
//        ResourceBundle bundle = ResourceBundle.getBundle("redis");
//        if (bundle == null)
//            throw new IllegalArgumentException("[redis.properties] is not found");
//
//        JedisPoolConfig config = new JedisPoolConfig();
////        config.setMaxActive(Integer.valueOf(bundle.getString("redis.pool.maxActive")));
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
//    /**
//     * Get Jedis resource from the pool
//     * @return
//     */
//    public static Jedis createInstance() {
//        try {
//            Jedis jedis = pool.getResource();
//
//            return jedis;
//        }catch ( Exception e ){
//            LOG.error("RedisPoolManager.createInstance Error",e);
//        }
//        return null;
//    }
//
//    /**
//     * Return the resource to pool
//     * @param jedis
//     */
//    public static void returnResource(Jedis jedis) {
//        pool.returnResource(jedis);
//    }
//
//}
