//package com.alibaba.rocketmq.storm;
//
//import com.alibaba.rocketmq.storm.redis.CacheManager;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Set;
//
///**
// * Created by robert on 2015/5/20.
// */
//public class TestCacheManager {
//    private CacheManager cacheManager;
//
//    @Before
//    public void init() {
//        cacheManager = CacheManager.getInstance();
//    }
//
//    @Test
//    public void set() {
//        Map<String, String> kValues = new HashMap<String, String>();
//        long times = System.currentTimeMillis();
//        kValues.put("testKey", "robert");
//        kValues.put("1_1_"+times, "1");
//        kValues.put("1_2_"+times, "2");
//        kValues.put("1_3_"+times, "3");
//        kValues.put("1_4_"+times, "4");
//        cacheManager.append("1_4_"+times, "four");
//
//        cacheManager.set(kValues);
//        System.out.println(cacheManager.getValue("4"));
//
//    }
//
//    @Test
//    public void calculateSize() {
//        System.out.println(cacheManager.getValue("four"));
//        System.out.println(cacheManager.calculateSize() + "");
//    }
//
//    @Test
//    public void getValue() {
//        String value = cacheManager.getValue("testKey");
//        Assert.assertNotNull(value);
//        Assert.assertEquals("robert", value);
//    }
//
//    @Test
//    public void getKeys(){
//        Set<String> keys = cacheManager.getKeys("1_*");
//        for (String key:keys) {
//            System.out.println(cacheManager.getValues(key));
//        }
//    }
//
//    @Test
//    public void remove(){
//        cacheManager.deleteValue("");
//    }
//}
