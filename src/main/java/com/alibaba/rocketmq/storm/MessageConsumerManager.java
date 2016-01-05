package com.alibaba.rocketmq.storm;

import com.alibaba.rocketmq.common.MixAll;
import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;

/**
 * @author Von Gosling
 */
public class MessageConsumerManager {

    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumerManager.class);

    MessageConsumerManager() {
    }

    public static MQConsumer getConsumerInstance(RocketMQConfig config,
                                                 MessageListener listener,
                                                 Boolean isPushlet) throws MQClientException {
        LOG.info("Begin to init consumer,instanceName->{},configuration->{}",
                new Object[]{config.getInstanceName(), config});
        LOG.info("----------------------------------------------- Enable SSL: " + System.getProperty("enable_ssl"));
        LOG.info("----------------------------------------------- rocketmq.namesrv.domain: " + MixAll.WS_DOMAIN_NAME);
        if (BooleanUtils.isTrue(isPushlet)) {
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(config.getGroupId());
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            pushConsumer.subscribe(config.getTopic(), config.getTopicTag());
            pushConsumer.setMessageModel(MessageModel.CLUSTERING);
            pushConsumer.registerMessageListener(listener);
            return pushConsumer;
        } else {
            DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer(config.getGroupId());
            pullConsumer.setMessageModel(MessageModel.CLUSTERING);
            pullConsumer.setPersistConsumerOffsetInterval(Integer.MAX_VALUE);

            // reset maximum hold up time.
            pullConsumer.setBrokerSuspendMaxTimeMillis(10 * 1000);
            pullConsumer.setConsumerPullTimeoutMillis(15 * 1000);

            return pullConsumer;
        }
    }
}
