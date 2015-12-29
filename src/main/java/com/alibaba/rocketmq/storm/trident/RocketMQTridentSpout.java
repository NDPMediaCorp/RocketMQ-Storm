package com.alibaba.rocketmq.storm.trident;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

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
public class RocketMQTridentSpout implements
        IPartitionedTridentSpout<List<MessageQueue>, ISpoutPartition, BatchMessage> {

    private static final long                                      serialVersionUID   = 8972193358178718167L;

    private static final Logger                                    LOG                = LoggerFactory.getLogger(RocketMQTridentSpout.class);

    private static final ConcurrentMap<String, List<MessageQueue>> cachedMessageQueue = new MapMaker().makeMap();
    private RocketMQConfig config;
    private volatile transient DefaultMQPullConsumer                                  consumer;

    public RocketMQTridentSpout() {
    }

    private MQPullConsumer getConsumer() throws MQClientException {
        if (null == consumer) {
            synchronized (this) {
                if (null == consumer) {
                    consumer = (DefaultMQPullConsumer) MessageConsumerManager.getConsumerInstance(config, null, false);
                    consumer.setInstanceName(UUID.randomUUID().toString());
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
            cachedMessageQueue.put(topic, cachedQueue);
        }
        return cachedQueue;
    }

    class RocketMQCoordinator implements Coordinator<List<MessageQueue>> {

        @Override
        public List<MessageQueue> getPartitionsForBatch() {
            List<MessageQueue> queue = Lists.newArrayList();
            try {
                queue = getMessageQueue(config.getTopic());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return queue;
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
        public List<ISpoutPartition> getOrderedPartitions(List<MessageQueue> allPartitionInfo) {
            List<ISpoutPartition> partition = Lists.newArrayList();
            for (final MessageQueue queue : allPartitionInfo) {
                partition.add(new ISpoutPartition() {
                    @Override
                    public String getId() {
                        return String.valueOf(queue.getQueueId());
                    }
                });
            }
            return partition;
        }

        /**
         * Emit a batch of tuples for a partition/transaction that's never been emitted before.
         * Return the metadata that can be used to reconstruct this partition/batch in the future.
         */
        @Override
        public BatchMessage emitPartitionBatchNew(TransactionAttempt tx,
                                                  TridentCollector collector,
                                                  ISpoutPartition partition,
                                                  BatchMessage lastPartitionMeta) {
            long index = 0;
            long pullBatchSize = 0;
            BatchMessage batchMessages = null;
            MessageQueue mq = null;
            try {
                mq = getMessageQueue(config.getTopic()).get(Integer.parseInt(partition.getId()));
                if (lastPartitionMeta == null) {
                    index = getConsumer().fetchConsumeOffset(mq, true);
                    index = index == -1 ? 0 : index;
                    long maxOffset = getConsumer().maxOffset(mq);
                    long diff = maxOffset - index;
                    pullBatchSize = diff > config.getPullBatchSize() ? config.getPullBatchSize() : diff;
                } else {
                    index = lastPartitionMeta.getOffset();
                    pullBatchSize = (int)(lastPartitionMeta.getNextOffset() - index - 1);
                }

                PullResult result = getConsumer().pullBlockIfNotFound(mq, config.getTopicTag(), index,
                        (int)pullBatchSize);
                List<MessageExt> msgs = result.getMsgFoundList();
                if (null != msgs && msgs.size() > 0) {
                    batchMessages = new BatchMessage(msgs, mq);
                    for (MessageExt msg : msgs) {
                        collector.emit(Lists.newArrayList(tx, msg));
                    }
                    getConsumer().updateConsumeOffset(mq, result.getMaxOffset());
                    assert result.getMaxOffset() == batchMessages.getNextOffset() - 1;
                }
            } catch (MQClientException | RemotingException | MQBrokerException
                    | InterruptedException e) {
                e.printStackTrace();
            }
            return batchMessages;
        }

        @Override
        public void refreshPartitions(List<ISpoutPartition> partitionResponsibilities) {

        }

        /**
         * Emit a batch of tuples for a partition/transaction that has been emitted before, using
         * the metadata created when it was first emitted.
         */
        @Override
        public void emitPartitionBatch(TransactionAttempt tx,
                                       TridentCollector collector,
                                       ISpoutPartition partition,
                                       BatchMessage partitionMeta) {

            try {
                MessageQueue mq = getMessageQueue(config.getTopic()).get(Integer.parseInt(partition.getId()));
                PullResult result = getConsumer().pullBlockIfNotFound(mq, config.getTopicTag(),
                        partitionMeta.getOffset(), partitionMeta.getMsgList().size());
                List<MessageExt> msgs = result.getMsgFoundList();
                if (null != msgs && msgs.size() > 0) {
                    getConsumer().updateConsumeOffset(mq, partitionMeta.getNextOffset());
                    for (MessageExt msg : msgs) {
                        collector.emit(Lists.newArrayList(tx, msg));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        @Override
        public void close() {
            LOG.info("close emitter!");
        }

    }

    @Override
    public Coordinator<List<MessageQueue>> getCoordinator(@SuppressWarnings("rawtypes") Map conf,
                                                          TopologyContext context) {
        return new RocketMQCoordinator();
    }

    @Override
    public Emitter<List<MessageQueue>, ISpoutPartition, BatchMessage> getEmitter(@SuppressWarnings("rawtypes") Map conf,
                                                                                 TopologyContext context) {
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
