package com.alibaba.rocketmq.storm.topology;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.rocketmq.storm.bolt.RocketMqBolt;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;
import com.alibaba.rocketmq.storm.domain.RocketMQSpouts;
import com.alibaba.rocketmq.storm.internal.tools.ConfigUtils;
import com.alibaba.rocketmq.storm.spout.StreamMessageSpout;
import com.alibaba.rocketmq.storm.spout.factory.RocketMQSpoutFactory;

public class SimpleTopology {
    private static final Logger LOG            = LoggerFactory.getLogger(SimpleTopology.class);

    private static final String BOLT_NAME      = "MQBolt";
    private static final String PROP_FILE_NAME = "mqspout.default.prop";

    private static Config       config         = null;
    private static boolean      isLocalMode    = false;

    public static void main(String[] args) throws Exception {
        config = ConfigUtils.init(PROP_FILE_NAME);
        TopologyBuilder builder = buildTopology(config);
        submitTopology(builder);
    }

    private static TopologyBuilder buildTopology(Config config) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        int boltParallel = NumberUtils.toInt((String) config.get("topology.bolt.parallel"), 1);

        int spoutParallel = NumberUtils.toInt((String) config.get("topology.spout.parallel"), 1);

        BoltDeclarer writerBolt = builder.setBolt(BOLT_NAME, new RocketMqBolt(), boltParallel);

        StreamMessageSpout defaultSpout = (StreamMessageSpout) RocketMQSpoutFactory.getSpout(RocketMQSpouts.STREAM.getValue());
        RocketMQConfig mqConfig = (RocketMQConfig) config.get(ConfigUtils.CONFIG_ROCKETMQ);
        defaultSpout.setConfig(mqConfig);

        String id = (String) config.get(ConfigUtils.CONFIG_TOPIC);
        builder.setSpout(id, defaultSpout, spoutParallel);

        writerBolt.shuffleGrouping(id);
        return builder;
    }

    private static void submitTopology(TopologyBuilder builder) {
        try {
            if (isLocalMode) {
                LocalCluster cluster = new LocalCluster();

                config.put(Config.STORM_CLUSTER_MODE, "local");
                cluster.submitTopology(String.valueOf(config.get("topology.name")), config,
                        builder.createTopology());

                Thread.sleep(50000);

                cluster.shutdown();
            } else {
                config.put(Config.STORM_CLUSTER_MODE, "distributed");
                config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Denable_ssl=true -Drocketmq.namesrv.domain=172.30.50.54 -Dlog.home=/home/storm/logs");

                //debug
                config.remove(ConfigUtils.CONFIG_ROCKETMQ);

                StormSubmitter.submitTopology(String.valueOf(config.get("topology.name")), config, builder.createTopology());
            }

        } catch (AlreadyAliveException e) {
            LOG.error(e.getMessage(), e.getCause());
        } catch (InvalidTopologyException e) {
            LOG.error(e.getMessage(), e.getCause());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e.getCause());
        }
    }
}
