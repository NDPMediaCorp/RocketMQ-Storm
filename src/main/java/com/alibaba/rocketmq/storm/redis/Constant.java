package com.alibaba.rocketmq.storm.redis;

public interface Constant {
    int PERIOD = 30;

    int NUMBERS = 60;

    int INCREMENT = 1;

    long BASE = 1L;

    String REDIS_CHANNEL = "EAGLE_CHANNEL";

    public static String KEY_OFFS_CONV_COUNT = "offs_conv_count";

    public static String KEY_OFFS_CLIK_COUNT = "offs_clik_count";

    public static String KEY_PRE_AFFS_IN_OFF = "affs_in_offer_";
}
