package com.alibaba.rocketmq.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.storm.hbase.HBaseClient;
import com.alibaba.rocketmq.storm.hbase.Helper;
import com.alibaba.rocketmq.storm.hbase.exception.HBasePersistenceException;
import com.alibaba.rocketmq.storm.model.AccessLog;
import com.alibaba.rocketmq.storm.model.AggregationResult;
import com.alibaba.rocketmq.storm.model.HBaseData;
import com.alibaba.rocketmq.storm.redis.Constant;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * NginxLogAggregationBolt Created with mythopoet.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/21 下午5:12
 * @desc
 */
public class NginxLogAggregationBolt implements IRichBolt,Constant {

    private static final long serialVersionUID = -8875616003742278953L;

    private static final Logger LOG = LoggerFactory.getLogger(NginxLogAggregationBolt.class);

    private OutputCollector outputCollector;

    private static final String TABLE_NAME = "eagle_log", COLUMN_FAMILY = "t", COLUMN_DC = "dc";

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private static final int HBASE_MAX_RETRY_TIMES = 5, REDIS_MAX_RETRY_TIMES = 5;

    /** ---------------------------offId@affId-----DC------Result------------ */
    private AtomicReference<HashMap<String,HashMap<String,AggregationResult>>> atomicReference = new AtomicReference<>();

    private AtomicLong counter = new AtomicLong(1L);

    private volatile boolean stop = false;

    @Override public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;

        HashMap<String,HashMap<String,AggregationResult>> map = new HashMap<>();
        while ( !atomicReference.compareAndSet(null,map) ){}

//        Thread persistThread = new Thread(new PersistTask());
//        persistThread.setName("PersistThread");
//        persistThread.setDaemon(true);
//        persistThread.start();

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
                processMsg(new String(msg.getBody(), Charset.forName("UTF-8")));
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

    public void processMsg(String msg) {
//        AccessLog logEntry = JSON.parseObject(msg, AccessLog.class);//TODO parse
//        HashMap<String, HashMap<String, AggregationResult>> parserAggResultMap = atomicReference.get();
//        String mapKey = ParserAggResult.spliceKey(logEntry.getOffer_id(), logEntry.getAffiliate_id());
//        HashMap<String, AggregationResult> dcResultMap;
//        AggregationResult aggregationResult;
//        if ( !parserAggResultMap.containsKey(mapKey) ) {
//            dcResultMap = new HashMap<>();
//            aggregationResult = new AggregationResult();
//            aggregationResult.setAvgResponseTime(NumberUtils.toDouble(logEntry.getUpstreamResponseTime()));
//            aggregationResult.setTotalCount(1);
//            aggregationResult.setOffId(logEntry.get());
//            aggregationResult.setAffId(logEntry.getAffiliate_id());
//        } else {
//            dcResultMap = parserAggResultMap.get(mapKey);
//            if ( !dcResultMap.containsKey(logEntry.getDc()) ) {
//                aggregationResult = new ParserAggResult();
//                aggregationResult.setAct(NumberUtils.toDouble(logEntry.getConnection_time()));
//                aggregationResult.setTc(1);
//                aggregationResult.setOffId(logEntry.getOffer_id());
//                aggregationResult.setAffId(logEntry.getAffiliate_id());
//            } else {
//                aggregationResult = dcResultMap.get(logEntry.getDc());
//                long totalCount = parserResult.getTc() + 1;
//                double avgConnTime = (parserResult.getAct() * parserResult.getTc() + NumberUtils.toDouble(logEntry.getConnection_time())) / totalCount;
//                aggregationResult.setAct(avgConnTime);
//                aggregationResult.setTc(totalCount);
//            }
//        }
//        dcResultMap.put(logEntry.getDc(), aggregationResult);
//        parserAggResultMap.put(mapKey, dcResultMap);
    }


//    class PersistTask implements Runnable {

//        private final CacheManager cacheManager = CacheManager.getInstance();
//
//        private HBaseClient hBaseClient = new HBaseClient();
//
//        public PersistTask() {
//            hBaseClient.start();
//        }
//
//        @Override
//        public void run() {
//            while ( !stop ) {
//                LOG.info("Start to persist aggregation result.");
//                try {
//                    HashMap<String, HashMap<String, ParserAggResult>> map = atomicReference.getAndSet(new HashMap<String, HashMap<String, ParserAggResult>>());
//
//                    if ( null == map || map.isEmpty() ) {
//                        LOG.info("No data to persist. Sleep to wait for the next cycle.");
//                        Thread.sleep(PERIOD * 1000);
//                        continue;
//                    }
//                    Calendar calendar = Calendar.getInstance();
//                    //Use Beijing Time Zone: GMT+8
//                    calendar.setTimeZone(TimeZone.getTimeZone("GMT+8"));
//                    String timestamp = calendar.getTimeInMillis() + "";
//                    List<HBaseData> hBaseDataList = new ArrayList<>();
//                    Map<String, String> redisCacheMap = new HashMap<>();
//                    HashMap<String, ParserAggResult> dcResultMap;
//                    ParserAggResult par;
//                    for ( Map.Entry<String, HashMap<String, ParserAggResult>> entry : map.entrySet() ) {
//                        String[] key = entry.getKey().split("@");
//                        dcResultMap = entry.getValue();
//                        String rowKey = Helper.generateKey(key[0], key[1], timestamp);
//                        StringBuilder connection = new StringBuilder();
//                        StringBuilder request = new StringBuilder();
//                        Map<String, byte[]> data = new HashMap<>();
//
//                        for ( Map.Entry<String, ParserAggResult> dcMapEntry : dcResultMap.entrySet() ) {
//                            par = dcMapEntry.getValue();
//                            connection.append(", ").append(dcMapEntry.getKey()).append(": ").append(par.getAct());
//                            request.append(", ").append(dcMapEntry.getKey()).append(": ").append(par.getTc());
//                        }
//
//                        String connectionResult = connection.toString();
//                        if ( connectionResult.contains(",") ) {
//                            connectionResult = connectionResult.substring(1);
//                        }
//                        connectionResult = "{" + connectionResult + "}";
//                        String requestResult = request.toString();
//                        if ( requestResult.contains(",") ) {
//                            requestResult = requestResult.substring(1);
//                        }
//                        requestResult = "{" + requestResult + "}";
//
//                        LOG.debug("[Connection] Key = " + connectionResult);
//                        LOG.debug("[request] Key = " + requestResult);
//
//                        data.put(COLUMN_CONNECTION, connectionResult.getBytes(DEFAULT_CHARSET));
//                        data.put(COLUMN_REQUEST, requestResult.getBytes(DEFAULT_CHARSET));
//                        redisCacheMap.put(rowKey, "{connection: " + connectionResult + ", request: " + requestResult + "}");
//                        HBaseData hBaseData = new HBaseData(TABLE_NAME, rowKey, COLUMN_FAMILY, data);
//                        hBaseDataList.add(hBaseData);
//                    }
//
//                    for ( int i = 0; i < REDIS_MAX_RETRY_TIMES; i++ ) {
//                        if ( !cacheManager.setKeyLive(redisCacheMap, PERIOD * NUMBERS) ) {
//                            if ( i < REDIS_MAX_RETRY_TIMES - 1 ) {
//                                LOG.error("Persisting to Redis failed, retry in " + (i + 1) + " seconds");
//                            } else {
//                                LOG.error("The following data are dropped due to failure to persist to Redis: %s", redisCacheMap);
//                            }
//
//                            Thread.sleep((i + 1) * 1000);
//                        } else {
//                            break;
//                        }
//                    }
//
//                    for ( int i = 0; i < HBASE_MAX_RETRY_TIMES; i++ ) {
//                        try {
//                            hBaseClient.insertBatch(hBaseDataList);
//                            break;
//                        } catch ( HBasePersistenceException e ) {
//                            if ( i < HBASE_MAX_RETRY_TIMES - 1 ) {
//                                LOG.error("Persisting aggregation data to HBase failed. Retry in " + (i + 1) + " second(s)");
//                            } else {
//                                LOG.error("The following aggregation data are dropped: %s", hBaseDataList);
//                            }
//                        }
//                        Thread.sleep((i + 1) * 1000);
//                    }
//
//                    cacheManager.publish(redisCacheMap, REDIS_CHANNEL);
//
//                    LOG.info("Persisting aggregation result done.");
//                } catch ( Exception e ) {
//                    LOG.error("Persistence of aggregated result failed.", e);
//                } finally {
//                    try {
//                        Thread.sleep(PERIOD * 1000);
//                    } catch ( InterruptedException e ) {
//                        LOG.error("PersistThread was interrupted.", e);
//                    }
//                }
//            }
//        }
//    }
}
