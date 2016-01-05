package com.alibaba.rocketmq.storm.trident;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.client.consumer.MQPullConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.spout.ISpoutPartition;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.storm.MessageConsumerManager;
import com.alibaba.rocketmq.storm.domain.BatchMessage;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;

/**
 * @author Von Gosling
 */
public class RocketMQTridentSpout implements IPartitionedTridentSpout<List<MessageQueue>, ISpoutPartition, BatchMessage> {

    private static final long serialVersionUID = 8972193358178718167L;

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQTridentSpout.class);

    private static final ConcurrentMap<String, List<MessageQueue>> cachedMessageQueue = new MapMaker().makeMap();

    private RocketMQConfig config;

    private volatile transient DefaultMQPullConsumer consumer;

    public RocketMQTridentSpout() {
    }

    private MQPullConsumer getConsumer() throws MQClientException {
        if (null == consumer) {
            synchronized (this) {
                if (null == consumer) {
                    consumer = (DefaultMQPullConsumer) MessageConsumerManager.getConsumerInstance(config, null, false);
                    consumer.start();
                }
            }
        }
        return consumer;
    }

    public RocketMQTridentSpout(RocketMQConfig config) throws MQClientException {
        this.config = config;
    }

    private List<MessageQueue> getMessageQueue(String topic) throws MQClientException {
        List<MessageQueue> cachedQueue = cachedMessageQueue.get(topic);
        if (cachedQueue == null) {
            // Fetches all message queues from name server.
            Set<MessageQueue> mqs = getConsumer().fetchSubscribeMessageQueues(topic);
            cachedQueue = Lists.newArrayList(mqs);
            Collections.sort(cachedQueue);
            if (LOG.isDebugEnabled()) {
                for (MessageQueue queue : cachedQueue) {
                    LOG.debug("Broker Name: {}, Queue ID: {}", queue.getBrokerName(), queue.getQueueId());
                }
            }
            cachedMessageQueue.put(topic, cachedQueue);
        }
        return cachedQueue;
    }

    class RocketMQCoordinator implements Coordinator<List<MessageQueue>> {

        @Override
        public List<MessageQueue> getPartitionsForBatch() {
            List<MessageQueue> messageQueues = Lists.newArrayList();
            try {
                messageQueues = getMessageQueue(config.getTopic());

                // Debug info
                if (LOG.isDebugEnabled()) {
                    LOG.debug("RocketMQCoordinator#getPartitionsForBatch");
                    for (MessageQueue messageQueue : messageQueues) {
                        LOG.debug("Message Queue: {}", messageQueue);
                    }
                }
            } catch (Exception e) {
                LOG.error("Failed to fetch message queues", e);
            }
            return messageQueues;
        }

        @Override
        public boolean isReady(long txId) {
            return true;
        }

        @Override
        public void close() {
            LOG.info("close coordinator!");
        }

    }

    class RocketMQEmitter implements Emitter<List<MessageQueue>, ISpoutPartition, BatchMessage> {

        @Override
        public List<ISpoutPartition> getOrderedPartitions(final List<MessageQueue> allPartitionInfo) {

            final String signature = getClass().getName() + "#getOrderedPartitions";
            LOG.debug("Enter: {}, Params: {}", signature, allPartitionInfo);
            Collections.sort(allPartitionInfo);
            final List<ISpoutPartition> partition = Lists.newArrayList();
            final AtomicInteger index = new AtomicInteger(0);
            for (final MessageQueue queue : allPartitionInfo) {
                partition.add(new ISpoutPartition() {
                    @Override
                    public String getId() {
                        String partitionId =  String.valueOf(index.getAndIncrement());
                        LOG.debug("Partition ID: {}, Broker Name: {}, Queue ID: {}",
                                partitionId, queue.getBrokerName(), queue.getQueueId());
                        return partitionId;
                    }
                });
            }
            return partition;
        }

        private BatchMessage handlePullResult(TransactionAttempt tx, TridentCollector collector, PullResult result, MessageQueue mq,
                                              BatchMessage lastPartitionMeta) throws MQClientException {
            final String signature = getClass().getName() + "#handlePullResult";
            LOG.debug("Enter {}, Params[lastPartitionMeta: {}, mq: {}], Thread: {}", signature, lastPartitionMeta, mq,
                    Thread.currentThread().getId());
            LOG.debug("Pull Status: {}", result.getPullStatus().name());
            switch (result.getPullStatus()) {
                case FOUND:
                    BatchMessage batchMessages = null;
                    List<MessageExt> msgs = result.getMsgFoundList();


                    // Figure out from and to offsets for this batch.
                    long fromOffset = Long.MAX_VALUE;
                    long toOffset = Long.MIN_VALUE;
                    for (MessageExt msg : msgs) {
                        if (msg.getQueueOffset() < fromOffset) {
                            fromOffset = msg.getQueueOffset();
                        }

                        if (msg.getQueueOffset() > toOffset) {
                            toOffset = msg.getQueueOffset();
                        }
                    }

                    LOG.debug("Offset[From(inclusive): {}, To(inclusive): {}, Min: {}, Max: {}, Next: {}], Message Count: {}",
                            fromOffset, toOffset, result.getMinOffset(), result.getMaxOffset(), result.getNextBeginOffset(),
                            msgs.size());

                    // Filter messages by tag.
                    if (null != config.getTopicTag() && !"*".equals(config.getTopicTag())) {
                        String[] tags = config.getTopicTag().split("\\|\\|");
                        List<MessageExt> filteredMsgs = Lists.newArrayList();
                        if (!msgs.isEmpty()) {
                            for (MessageExt msg : msgs) {
                                for (String tag : tags) {
                                    if (tag.equals(msg.getTags())) {
                                        filteredMsgs.add(msg);
                                        break;
                                    }
                                }
                            }
                        }
                        msgs = filteredMsgs;
                    }

                    batchMessages = new BatchMessage(msgs, mq);
                    batchMessages.setOffset(fromOffset);
                    batchMessages.setNextOffset(result.getNextBeginOffset());

                    for (MessageExt msg : msgs) {
                        collector.emit(Lists.newArrayList(tx, msg));
                    }

                    getConsumer().updateConsumeOffset(mq, result.getNextBeginOffset(), true);
                    DefaultMQPullConsumer defaultMQPullConsumer = (DefaultMQPullConsumer) getConsumer();
                    defaultMQPullConsumer.getOffsetStore().persist(mq);
                    return batchMessages;

                case NO_NEW_MSG:
                    LOG.debug("No new messages for this pull request.");
                    break;

                case NO_MATCHED_MSG:
                    LOG.debug("No matched messages for this pull request");
                    break;

                case OFFSET_ILLEGAL:
                    LOG.error("Offset illegal, please notify RocketMQ Development Team");
                    return lastPartitionMeta;

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

            LOG.debug("Next Offset: {}", result.getNextBeginOffset());

            BatchMessage batchMessage = new BatchMessage();
            batchMessage.setOffset(result.getNextBeginOffset());
            batchMessage.setNextOffset(result.getNextBeginOffset());
            return batchMessage;
        }

        /**
         * Emit a batch of tuples for a partition/transaction that's never been emitted before.
         * Return the metadata that can be used to reconstruct this partition/batch in the future.
         */
        @Override
        public BatchMessage emitPartitionBatchNew(TransactionAttempt tx, //
                                                  TridentCollector collector, //
                                                  ISpoutPartition partition, //
                                                  BatchMessage lastPartitionMeta //
        ) {
            final String signature = getClass().getName() + "#emitPartitionBatchNew";
            LOG.debug("Enter: {}, Params:[tx: {}, partition: {}, lastPartitionMeta: {}] Thread ID: {}", signature, tx, partition.getId(), lastPartitionMeta,
                    Thread.currentThread().getId());

            try {
                MessageQueue mq = getMessageQueue(config.getTopic()).get(Integer.parseInt(partition.getId()));

                long index = 0;
                if (null == lastPartitionMeta) {
                    index = getConsumer().fetchConsumeOffset(mq, true);
                    if (index < 0) {
                        index = 0;
                    }
                } else {
                    index = lastPartitionMeta.getNextOffset();
                }

                LOG.debug("Begin to pull[MessageQueue: {}, tag: {}, index: {}, maxNum: {}, thread:{}]", mq, config.getTopicTag(), index,
                        config.getPullBatchSize(), Thread.currentThread().getId());
                PullResult result = getConsumer().pullBlockIfNotFound(mq, config.getTopicTag(), index, config.getPullBatchSize());
                return handlePullResult(tx, collector, result, mq, lastPartitionMeta);
            } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                LOG.error("Pull Failed.", e);
                return lastPartitionMeta;
            }
        }

        @Override
        public void refreshPartitions(List<ISpoutPartition> partitionResponsibilities) {

        }

        /**
         * Emit a batch of tuples for a partition/transaction that has been emitted before, using
         * the metadata created when it was first emitted.
         */
        @Override
        public void emitPartitionBatch(TransactionAttempt tx, //
                                       TridentCollector collector, //
                                       ISpoutPartition partition, //
                                       BatchMessage partitionMeta //
        ) {
            final String signature = getClass().getName() + "#emitPartitionBatch";
            LOG.debug("Thread ID:{}, Enter: {}, Params:[tx: {}, partition: {}, partitionMeta: {}]", signature, tx, partition.getId(), partitionMeta, Thread.currentThread().getId());
            try {
                MessageQueue mq = getMessageQueue(config.getTopic()).get(Integer.parseInt(partition.getId()));
                int batchSize = (int) (partitionMeta.getNextOffset() - partitionMeta.getOffset());
                if (batchSize <= 0) {
                    // Skip this batch.
                    return;
                }

                PullResult result = getConsumer().pullBlockIfNotFound(mq, config.getTopicTag(), partitionMeta.getOffset(), batchSize);
                handlePullResult(tx, collector, result, mq, partitionMeta);
            } catch (Exception e) {
                LOG.error("Pull failed", e);
            }
        }

        @Override
        public void close() {
            LOG.info("close emitter!");
        }

    }

    @Override
    public Coordinator<List<MessageQueue>> getCoordinator(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
        return new RocketMQCoordinator();
    }

    @Override
    public Emitter<List<MessageQueue>, ISpoutPartition, BatchMessage> getEmitter(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
        return new RocketMQEmitter();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("tId", "message");
    }

}
