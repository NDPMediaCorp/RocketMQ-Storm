package com.alibaba.rocketmq.storm.spout.batch;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.IBatchSpout;
import com.alibaba.jstorm.batch.util.BatchCommon;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

public class BatchRocketMQSpout implements IBatchSpout{
	private static final long serialVersionUID = 5720810158625748041L;

	private static final Logger LOG = Logger.getLogger(BatchRocketMQSpout.class);
	
	private Map conf;
	private String taskName;
	private int taskParallel;

	private transient RocketMQConsumer rocketMQConsumer;
    private RocketMQConfig rocketMQConfig;

	

	public BatchRocketMQSpout(RocketMQConfig rocketMQConfig) {
		this.rocketMQConfig = rocketMQConfig;
	}
	
	public void initMetaClient() throws Exception {
		ClusterState zkClient = BatchCommon.getZkClient(conf);
        rocketMQConsumer = new RocketMQConsumer(rocketMQConfig, zkClient,taskParallel);
        rocketMQConsumer.init();
		LOG.info("Successfully init meta client " + taskName);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.conf = stormConf;

		taskName = context.getThisComponentId() + "_" + context.getThisTaskId();
		taskParallel = context.getComponentTasks(context.getThisComponentId())
				.size();

		try {
			initMetaClient();
		} catch (Exception e) {
			LOG.info("Failed to init Meta Client,", e);
			throw new RuntimeException(e);
		}

		LOG.info(taskName + " successfully do prepare ");
	}

	public void emitBatch(BatchId batchId, BasicOutputCollector collector) {
		List<MessageExt> msgs = rocketMQConsumer.batchFetchMessage();
		for (MessageExt msgExt : msgs) {
			collector.emit(new Values(batchId, msgExt));
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		String streamId = input.getSourceStreamId();
		if (streamId.equals(BatchMetaRebalance.REBALANCE_STREAM_ID)) {
			try {
				rocketMQConsumer.rebalanceMqList();
			} catch (Exception e) {
				LOG.warn("Failed to do rebalance operation", e);
			}
		}else {
			BatchId batchId = (BatchId) input.getValue(0);
			emitBatch(batchId, collector);
		}
		
	}

	@Override
	public void cleanup() {
		rocketMQConsumer.cleanup();

	}

	@Override
	public byte[] commit(BatchId id) throws FailedException {
		return rocketMQConsumer.commit(id);
	}

	@Override
	public void revert(BatchId id, byte[] commitResult) {
		rocketMQConsumer.revert(id, commitResult);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("BatchId", "MessageExt"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	
	

}
