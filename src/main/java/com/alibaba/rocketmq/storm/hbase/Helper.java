package com.alibaba.rocketmq.storm.hbase;

public final class Helper {

    public static void checkNotNullNorEmpty(String name, String s) {
        if (null == s || s.trim().length() == 0) {
            throw new IllegalArgumentException(name + " should not null nor empty.");
        }
    }

    public static String generateKeyForRedis(String offerId, String affiliateId, String timestamp) {
        checkNotNullNorEmpty("offerId", offerId);
        checkNotNullNorEmpty("affiliateId", affiliateId);
        checkNotNullNorEmpty("timestamp", timestamp);
        String comb = offerId + "_" + affiliateId;
        long endWith = Long.MAX_VALUE - Long.parseLong(timestamp);
        return comb.hashCode() % 64 + "_" + comb + "_" + endWith;
    }

    public static String generateKeyForHBase(String offerId, String affiliateId) {
        checkNotNullNorEmpty("offerId", offerId);
        checkNotNullNorEmpty("affiliateId", affiliateId);
        return offerId + "_" + affiliateId;
    }

}
