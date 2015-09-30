package com.alibaba.rocketmq.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.storm.hbase.HBaseClient;
import com.alibaba.rocketmq.storm.hbase.Helper;
import com.alibaba.rocketmq.storm.hbase.exception.HBasePersistenceException;
import com.alibaba.rocketmq.storm.model.AccessLog;
import com.alibaba.rocketmq.storm.model.AggregationResult;
import com.alibaba.rocketmq.storm.model.HBaseData;
import com.alibaba.rocketmq.storm.redis.Constant;
import com.alibaba.rocketmq.storm.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * NginxLogAggregationBolt Created with rocetmq-storm.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/21 下午5:12
 * @desc
 */
public class NginxLogAggregationBolt implements IRichBolt, Constant {

    private static final long serialVersionUID = -8875616003742278953L;

    private static final Logger LOG = LoggerFactory.getLogger(NginxLogAggregationBolt.class);

    private OutputCollector outputCollector;

    private static final String TABLE_NAME = "eagle_log", COLUMN_FAMILY = "nginx", COLUMN_NCLICK = "nclick", COLUMN_NCONV = "nconv";

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private static final int HBASE_MAX_RETRY_TIMES = 5, REDIS_MAX_RETRY_TIMES = 5;

    /** ---------------------------offId_affId-----click/conv@region------Result------------ */
    private AtomicReference<HashMap<String, HashMap<String, AggregationResult>>> atomicReferenceRegion = new AtomicReference<>();

    private AtomicLong counter = new AtomicLong(1L);

    private volatile boolean stop = false;

    @Override public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;

        HashMap<String, HashMap<String, AggregationResult>> mapRegion = new HashMap<>();
        while ( !atomicReferenceRegion.compareAndSet(null, mapRegion) ) {}

        Thread persistThread = new Thread(new PersistTask());
        persistThread.setName("PersistThread");
        persistThread.setDaemon(true);
        persistThread.start();

    }

    @Override public void execute(Tuple input) {
        if ( counter.incrementAndGet() % 100000 == 0 ) {
            LOG.info("100000 tuples aggregated.");
        }

        Object msgObj = input.getValue(0);
        //        Object msgStat = input.getValue(1);
        try {
            if ( msgObj instanceof MessageExt ) {
                MessageExt msg = (MessageExt) msgObj;
                AccessLog accessLog = new AccessLog(new String(msg.getBody(), Charset.forName("UTF-8")));
                processMsg4Region(accessLog);
            } else {
                LOG.error("The first value in tuple should be MessageExt object");
            }
        } catch ( Exception e ) {
            LOG.error("Failed to handle Message", e);
            outputCollector.fail(input);
            return;
        }

        outputCollector.ack(input);
    }

    @Override public void cleanup() {
        stop = true;
    }

    @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void processMsg4Region(AccessLog accessLog) {
        if ( !accessLog.isFull() ) {
            return;
        }
        HashMap<String, HashMap<String, AggregationResult>> parserAggResultMap = atomicReferenceRegion.get();
        String offerId = accessLog.offerId();
        String affiliateId = accessLog.affiliateId();
        String mapKey = Helper.generateKeyForHBase(offerId, affiliateId);
        HashMap<String, AggregationResult> regionResultMap;
        String regionMapKey = accessLog.isClick() ? COLUMN_NCLICK + "@" + accessLog.getRegion() : COLUMN_NCONV + "@" + accessLog.getRegion();
        AggregationResult aggregationResult;
        if ( !parserAggResultMap.containsKey(mapKey) ) {
            regionResultMap = new HashMap<>();
            aggregationResult = new AggregationResult();
            aggregationResult.setTotalCount(1);
            aggregationResult.setOffId(accessLog.offerId());
            aggregationResult.setAffId(accessLog.affiliateId());
            aggregationResult.setClick(accessLog.isClick());
        } else {
            regionResultMap = parserAggResultMap.get(mapKey);
            if ( !regionResultMap.containsKey(regionMapKey) ) {
                aggregationResult = new AggregationResult();
                aggregationResult.setTotalCount(1);
                aggregationResult.setOffId(accessLog.offerId());
                aggregationResult.setAffId(accessLog.affiliateId());
                aggregationResult.setClick(accessLog.isClick());
            } else {
                aggregationResult = regionResultMap.get(regionMapKey);
                aggregationResult.setTotalCount(aggregationResult.getTotalCount() + 1);
            }
        }
        regionResultMap.put(regionMapKey, aggregationResult);
        parserAggResultMap.put(mapKey, regionResultMap);
    }

    class PersistTask implements Runnable {

        private HBaseClient hBaseClient = new HBaseClient();

        public PersistTask() {
            hBaseClient.start();
        }

        @Override
        public void run() {
            while ( !stop ) {
                LOG.info("Start to persist aggregation result.");
                try {
                    HashMap<String, HashMap<String, AggregationResult>> regionMap = atomicReferenceRegion.getAndSet(
                            new HashMap<String, HashMap<String, AggregationResult>>());

                    if ( null == regionMap || regionMap.isEmpty() ) {
                        LOG.info("No data to persist. Sleep to wait for the next cycle.");
                        Thread.sleep(PERIOD * 1000);
                        continue;
                    }
                    List<HBaseData> hBaseDataList = new ArrayList<>();
                    Map<String, String> redisCacheMap = new HashMap<>();
                    HashMap<String, AggregationResult> regionResultMap;
                    AggregationResult result;
                    for ( Map.Entry<String, HashMap<String, AggregationResult>> entry : regionMap.entrySet() ) {
                        regionResultMap = entry.getValue();
                        String rowKey = entry.getKey();
                        StringBuilder nclick = new StringBuilder();
                        StringBuilder nconv = new StringBuilder();
                        Map<String, byte[]> data = new HashMap<>();

                        for ( Map.Entry<String, AggregationResult> regionMapEntry : regionResultMap.entrySet() ) {
                            result = regionMapEntry.getValue();
                            String[] region = regionMapEntry.getKey().split("@");
                            if ( null == region || region.length <= 1 ){
                                continue;
                            }
                            if ( COLUMN_NCLICK.equals(region[0]) ) {
                                nclick.append(", ").append(region[1]).append(": ").append(result.getTotalCount());
                            } else {
                                nconv.append(", ").append(region[1]).append(": ").append(result.getTotalCount());
                            }
                        }

                        String nclickResult = nclick.toString();
                        if ( nclickResult.contains(",") ) {
                            nclickResult = nclickResult.substring(1);
                        }
                        nclickResult = "{" + nclickResult + "}";
                        String nconvResult = nconv.toString();
                        if ( nconvResult.contains(",") ) {
                            nconvResult = nconvResult.substring(1);
                        }
                        nconvResult = "{" + nconvResult + "}";

                        LOG.debug("[nClick] Key = " + nclickResult);
                        LOG.debug("[nConv] Key = " + nconvResult);

                        data.put(COLUMN_NCLICK, nclickResult.getBytes(DEFAULT_CHARSET));
                        data.put(COLUMN_NCONV, nconvResult.getBytes(DEFAULT_CHARSET));
                        redisCacheMap.put(rowKey, "{connection: " + nclickResult + ", request: " + nconvResult + "}");
                        HBaseData hBaseData = new HBaseData(TABLE_NAME, rowKey, COLUMN_FAMILY, data);
                        hBaseDataList.add(hBaseData);
                    }

                    for ( int i = 0; i < REDIS_MAX_RETRY_TIMES; i++ ) {
                        if ( !RedisClient.getInstance().setKeyLive(redisCacheMap, PERIOD * NUMBERS) ) {
                            if ( i < REDIS_MAX_RETRY_TIMES - 1 ) {
                                LOG.error("Persisting to Redis failed, retry in " + (i + 1) + " seconds");
                            } else {
                                LOG.error("The following data are dropped due to failure to persist to Redis: %s", redisCacheMap);
                            }

                            Thread.sleep((i + 1) * 1000);
                        } else {
                            break;
                        }
                    }

                    for ( int i = 0; i < HBASE_MAX_RETRY_TIMES; i++ ) {
                        try {
                            hBaseClient.insertBatch(hBaseDataList);
                            break;
                        } catch ( HBasePersistenceException e ) {
                            if ( i < HBASE_MAX_RETRY_TIMES - 1 ) {
                                LOG.error("Persisting aggregation data to HBase failed. Retry in " + (i + 1) + " second(s)");
                            } else {
                                LOG.error("The following aggregation data are dropped: %s", hBaseDataList);
                            }
                        }
                        Thread.sleep((i + 1) * 1000);
                    }

                    RedisClient.getInstance().publish(redisCacheMap, REDIS_CHANNEL);

                    LOG.info("Persisting aggregation result done.");
                } catch ( Exception e ) {
                    LOG.error("Persistence of aggregated result failed.", e);
                } finally {
                    try {
                        Thread.sleep(PERIOD * 1000);
                    } catch ( InterruptedException e ) {
                        LOG.error("PersistThread was interrupted.", e);
                    }
                }
            }
        }
    }
}
