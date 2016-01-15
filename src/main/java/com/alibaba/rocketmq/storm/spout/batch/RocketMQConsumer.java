package com.alibaba.rocketmq.storm.spout.batch;

import backtype.storm.topology.FailedException;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.ICommitter;
import com.alibaba.jstorm.batch.util.BatchDef;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.storm.MessageConsumerManager;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

public class RocketMQConsumer implements ICommitter {

    private static final Logger LOG = Logger.getLogger(RocketMQConsumer.class);

    private final RocketMQConfig rocketMQConfig;

    private final int taskParallel;

    private static final ConcurrentMap<String, List<MessageQueue>> cachedMessageQueue = new MapMaker().makeMap();

    private Map<MessageQueue, Long> currentOffsets;

    private Map<MessageQueue, Long> frontOffsets;

    private Map<MessageQueue, Long> backendOffset;

    private DefaultMQPullConsumer consumer;

    private final ClusterState zkClient;

    public RocketMQConsumer(RocketMQConfig config, ClusterState zkClient, int taskParall) {
        this.rocketMQConfig = config;
        this.zkClient = zkClient;
        this.taskParallel = taskParall;
    }

    public void initMetaConsumer() throws MQClientException {
        LOG.info("RocketMQConfig:" + rocketMQConfig);

        consumer = (DefaultMQPullConsumer) MessageConsumerManager.getConsumerInstance(rocketMQConfig, null, false);
        consumer.start();

        LOG.info("Successfully start rocketMQ consumer");
    }

    public void init() throws Exception {
        initMetaConsumer();
        frontOffsets = initOffset();
        currentOffsets = frontOffsets;
        backendOffset = new HashMap<>();
        backendOffset.putAll(frontOffsets);

    }

    private List<MessageQueue> getMessageQueue(String topic) throws MQClientException {
        List<MessageQueue> cachedQueue = cachedMessageQueue.get(topic);
        if ( cachedQueue == null ) {
            // Fetches all message queues from name server.
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
            cachedQueue = Lists.newArrayList(mqs);
            Collections.sort(cachedQueue);
            cachedMessageQueue.put(topic, cachedQueue);
        }
        return cachedQueue;
    }

    protected Map<MessageQueue, Long> initOffset() throws MQClientException {
        List<MessageQueue> queues = getMessageQueue(rocketMQConfig.getTopic());

        Map<MessageQueue, Long> ret = new HashMap<>();

        for ( MessageQueue mq : queues ) {
            long offset = getOffsetFromZk(mq);
            ret.put(mq, offset);
        }
        return ret;
    }

    protected String getZkPath(MessageQueue mq) {
        StringBuffer sb = new StringBuffer();
        sb.append(BatchDef.ZK_SEPERATOR);
        sb.append(rocketMQConfig.getGroupId());
        sb.append(BatchDef.ZK_SEPERATOR);
        sb.append(mq.getBrokerName());
        sb.append(BatchDef.ZK_SEPERATOR);
        sb.append(mq.getQueueId());

        return sb.toString();
    }

    protected long getOffsetFromZk(MessageQueue mq) {
        String path = getZkPath(mq);

        try {
            if ( zkClient.node_existed(path, false) == false ) {
                LOG.info("No zk node of " + path);
                return 0;
            }
            byte[] data = zkClient.get_data(path, false);
            String value = new String(data);
            long ret = Long.valueOf(value);
            return ret;
        } catch ( Exception e ) {
            LOG.warn("Failed to get offset,", e);
            return 0;
        }
    }

    protected void updateOffsetToZk(MessageQueue mq, Long offset) throws Exception {
        String path = getZkPath(mq);
        byte[] data = String.valueOf(offset).getBytes();
        zkClient.set_data(path, data);

    }

    protected void updateOffsetToZk(Map<MessageQueue, Long> mqs) throws Exception {
        for ( Entry<MessageQueue, Long> entry : mqs.entrySet() ) {
            MessageQueue mq = entry.getKey();
            Long offset = entry.getValue();

            updateOffsetToZk(mq, offset);
        }

        LOG.info("Update zk offset," + mqs);
    }

    protected void switchOffsetMap() {
        Map<MessageQueue, Long> tmp = frontOffsets;
        frontOffsets = backendOffset;
        backendOffset = tmp;
        currentOffsets = frontOffsets;
    }

    @Override
    public byte[] commit(BatchId id) throws FailedException {
        try {
            updateOffsetToZk(currentOffsets);
            switchOffsetMap();
        } catch ( Exception e ) {
            LOG.warn("Failed to update offset to ZK", e);
            throw new FailedException(e);
        }
        return null;
    }

    @Override
    public void revert(BatchId id, byte[] commitResult) {
        try {
            switchOffsetMap();
            updateOffsetToZk(currentOffsets);

        } catch ( Exception e ) {
            LOG.warn("Failed to update offset to ZK", e);
            throw new FailedException(e);
        }
    }

    /**
     * rebalanceMqList must run after commit
     *
     * @throws com.alibaba.rocketmq.client.exception.MQClientException
     */
    public void rebalanceMqList() throws Exception {
        LOG.info("Begin to do rebalance operation");
        List<MessageQueue> newMqs = getMessageQueue(rocketMQConfig.getTopic());

        Set<MessageQueue> oldMqs = currentOffsets.keySet();

        if ( oldMqs.equals(newMqs) == true ) {
            LOG.info("No change of meta queues " + newMqs);
            return;
        }

        Set<MessageQueue> removeMqs = new HashSet<MessageQueue>();
        removeMqs.addAll(oldMqs);
        removeMqs.removeAll(newMqs);

        Set<MessageQueue> addMqs = new HashSet<MessageQueue>();
        addMqs.addAll(newMqs);
        addMqs.removeAll(oldMqs);

        LOG.info("Remove " + removeMqs);
        for ( MessageQueue mq : removeMqs ) {
            Long offset = frontOffsets.remove(mq);
            updateOffsetToZk(mq, offset);
            backendOffset.remove(mq);

        }

        LOG.info("Add " + addMqs);
        for ( MessageQueue mq : addMqs ) {
            long offset = getOffsetFromZk(mq);
            frontOffsets.put(mq, offset);
            backendOffset.put(mq, offset);
        }
    }

    public List<MessageExt> batchFetchMessage() {
        List<MessageExt> ret = new ArrayList<>();

        for ( Entry<MessageQueue, Long> entry : currentOffsets.entrySet() ) {
            MessageQueue mq = entry.getKey();
            Long offset = entry.getValue();

            int fetchSize = 0;
            int pullSize = Math.min(rocketMQConfig.getBatchMsgSize() / (taskParallel * currentOffsets.size()), 32);

            while ( fetchSize < pullSize ) {

                PullResult pullResult = null;
                try {
                    pullResult = consumer.pullBlockIfNotFound(mq, rocketMQConfig.getTopicTag(), offset, pullSize);

                    switch ( pullResult.getPullStatus() ) {
                    case FOUND:
                        List<MessageExt> msgs = pullResult.getMsgFoundList();
                        ret.addAll(msgs);
                        fetchSize += msgs.size();

                        // Figure out from and to offsets for this batch.
                        long fromOffset = Long.MAX_VALUE;
                        long toOffset = Long.MIN_VALUE;
                        for ( MessageExt msg : msgs ) {
                            if ( msg.getQueueOffset() < fromOffset ) {
                                fromOffset = msg.getQueueOffset();
                            }

                            if ( msg.getQueueOffset() > toOffset ) {
                                toOffset = msg.getQueueOffset();
                            }
                        }

                        consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset(), true);
                        consumer.getOffsetStore().persist(mq);
                        continue;

                    case NO_NEW_MSG:
                        LOG.debug("No new messages for this pull request.");
                        break;

                    case NO_MATCHED_MSG:
                        LOG.debug("No matched messages for this pull request");
                        break;

                    case OFFSET_ILLEGAL:
                        LOG.error("Offset illegal, please notify RocketMQ Development Team");
                        break;

                    case SLAVE_LAG_BEHIND:
                        LOG.warn("Master node is down and slave replication is lagged behind.");
                        break;

                    case SUBSCRIPTION_NOT_LATEST:
                        LOG.error("Subscription is not latest, please notify RocketMQ Development Team");
                        break;

                    default:
                        LOG.error("Unexpected execution, please notify RocketMQ Development Team");
                        break;
                    }
                } catch ( Exception e ) {
                    LOG.warn("Failed to fetch messages of " + mq + ":" + offset, e);
                    break;
                }
            }

            backendOffset.put(mq, offset);
        }

        return ret;
    }

    public void cleanup() {
        consumer.shutdown();
    }
}
