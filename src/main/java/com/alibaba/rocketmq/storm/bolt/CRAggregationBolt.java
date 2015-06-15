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
import com.alibaba.rocketmq.storm.internal.tools.CRLogUtils;
import com.alibaba.rocketmq.storm.model.CRLog;
import com.alibaba.rocketmq.storm.model.HBaseData;
import com.alibaba.rocketmq.storm.redis.CacheManager;
import com.alibaba.rocketmq.storm.redis.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 *     To aggregate CR grouping by offer ID, affiliate ID, event code.
 * </p>
 *
 * @version 1.0
 * @since 1.0
 * @author Li Zhanhui
 */
public class CRAggregationBolt implements IRichBolt, Constant {
    private static final long serialVersionUID = 7591260982890048043L;

    private static final Logger LOG = LoggerFactory.getLogger(CRAggregationBolt.class);

    private OutputCollector collector;

    private static final String DATE_FORMAT = "yyyyMMddHHmmss";

    private static final String TABLE_NAME = "eagle_log";

    private static final String COLUMN_FAMILY = "t";

    private static final String COLUMN_CLICK = "click";
    private static final String COLUMN_CONVERSION = "conversion";
    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private static final int HBASE_MAX_RETRY_TIMES = 5;

    private static final int REDIS_MAX_RETRY_TIMES = 5;

    private AtomicReference<HashMap<String, HashMap<String, HashMap<String, Long>>>>
            atomicReference = new AtomicReference<>();

    private AtomicLong counter = new AtomicLong(1L);

    private volatile boolean stop = false;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

        /**
         * We use one worker, one executor and one task strategy.
         */
        HashMap<String /* offer_id */, HashMap<String /* affiliate_id*/, HashMap<String /* event_code*/, Long>>>
                resultMap = new HashMap<>();

        while (!atomicReference.compareAndSet(null, resultMap)) {
        }

        Thread persistThread = new Thread(new PersistTask());
        persistThread.setName("PersistThread");
        persistThread.setDaemon(true);
        persistThread.start();
    }

    @Override
    public void execute(Tuple input) {

        if (counter.incrementAndGet() % 10000 == 0) {
            LOG.info("10000 tuples aggregated.");
        }

        Object msgObj = input.getValue(0);
        Object msgStat = input.getValue(1);
        try {
            if (msgObj instanceof MessageExt) {
                MessageExt msg = (MessageExt) msgObj;
                CRLog logEntry = JSON.parseObject(new String(msg.getBody(), Charset.forName("UTF-8")), CRLog.class);
                if (CRLogUtils.checkCRLog(logEntry)) {
                    HashMap<String, HashMap<String, HashMap<String, Long>>> map = atomicReference.get();
                    if (!map.containsKey(logEntry.getOffer_id())) {
                        HashMap<String, HashMap<String, Long>> affMap = new HashMap<>();
                        HashMap<String, Long> eventMap = new HashMap<>();
                        eventMap.put(logEntry.getEvent_code(), BASE);
                        affMap.put(logEntry.getAffiliate_id(), eventMap);
                        map.put(logEntry.getOffer_id(), affMap);
                    } else {
                        HashMap<String, HashMap<String, Long>> affMap = map.get(logEntry.getOffer_id());
                        if (affMap.containsKey(logEntry.getAffiliate_id())) {
                            HashMap<String, Long> eventMap = affMap.get(logEntry.getAffiliate_id());
                            String eventCode = logEntry.getEvent_code();
                            if (eventMap.containsKey(eventCode)) {
                                eventMap.put(eventCode, eventMap.get(eventCode) + INCREMENT);
                            } else {
                                eventMap.put(logEntry.getEvent_code(), BASE);
                            }
                        } else {
                            HashMap<String, Long> eventMap = new HashMap<>();
                            eventMap.put(logEntry.getEvent_code(), BASE);
                            affMap.put(logEntry.getAffiliate_id(), eventMap);
                        }
                    }
                }
            } else {
                LOG.error("The first value in tuple should be MessageExt object");
            }
        } catch (Exception e) {
            LOG.error("Failed to handle Message", e);
            collector.fail(input);
            return;
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {
        stop = true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    class PersistTask implements Runnable {
        private final CacheManager cacheManager = CacheManager.getInstance();

        private HBaseClient hBaseClient = new HBaseClient();

        public PersistTask() {
            hBaseClient.start();
        }

        @Override
        public void run() {
            while (!stop) {
                LOG.info("Start to persist aggregation result.");
                try {
                    HashMap<String, HashMap<String, HashMap<String, Long>>> map =
                            atomicReference.getAndSet(new HashMap<String, HashMap<String, HashMap<String, Long>>>());

                    if (null == map || map.isEmpty()) {
                        LOG.info("No data to persist. Sleep to wait for the next cycle.");
                        Thread.sleep(PERIOD * 1000);
                        continue;
                    }

                    Calendar calendar = Calendar.getInstance();
                    //Use Beijing Time Zone: GMT+8
                    calendar.setTimeZone(TimeZone.getTimeZone("GMT+8"));
                    String timestamp = calendar.getTimeInMillis() + "";
                    List<HBaseData> hBaseDataList = new ArrayList<>();
                    Map<String, String> redisCacheMap = new HashMap<>();
                    for (Map.Entry<String, HashMap<String, HashMap<String, Long>>> row : map.entrySet()) {
                        String offerId = row.getKey();
                        HashMap<String, HashMap<String, Long>> affMap = row.getValue();
                        for (Map.Entry<String, HashMap<String, Long>> affRow : affMap.entrySet()) {
                            String affId = affRow.getKey();
                            HashMap<String, Long> eventMap = affRow.getValue();
                            String rowKey = Helper.generateKey(offerId, affId, timestamp);
                            StringBuilder click = new StringBuilder();
                            click.append("{");

                            StringBuilder conversion = new StringBuilder();
                            conversion.append("{");
                            Map<String, byte[]> data = new HashMap<String, byte[]>();
                            for (Map.Entry<String, Long> eventRow : eventMap.entrySet()) {
                                String event = eventRow.getKey();
                                if (event.startsWith("C")) {
                                    click.append(event).append(": ").append(eventRow.getValue()).append(", ");
                                } else {
                                    conversion.append(event).append(": ").append(eventRow.getValue()).append(", ");
                                }
                            }

                            if (click.length() > 2) {
                                click.replace(click.length() - 2, click.length(), "}");
                            } else {
                                click.append("}");
                            }

                            if (conversion.length() > 2) {
                                conversion.replace(conversion.length() - 2, conversion.length(), "}");
                            } else {
                                conversion.append("}");
                            }

                            LOG.debug("[Click] Key = " + click.toString());
                            LOG.debug("[Conversion] Key = " + conversion.toString());

                            data.put(COLUMN_CLICK, click.toString().getBytes(DEFAULT_CHARSET));
                            data.put(COLUMN_CONVERSION, conversion.toString().getBytes(DEFAULT_CHARSET));
                            redisCacheMap.put(rowKey, "{click: " + click + ", conversion: " + conversion + "}");
                            HBaseData hBaseData = new HBaseData(TABLE_NAME, rowKey, COLUMN_FAMILY, data);
                            hBaseDataList.add(hBaseData);
                        }
                    }

                    for (int i = 0; i < REDIS_MAX_RETRY_TIMES; i++) {
                        if (!cacheManager.setKeyLive(redisCacheMap, PERIOD * NUMBERS)) {
                            if (i < REDIS_MAX_RETRY_TIMES - 1) {
                                LOG.error("Persisting to Redis failed, retry in " + (i + 1) + " seconds");
                            } else {
                                LOG.error("The following data are dropped due to failure to persist to Redis: %s", redisCacheMap);
                            }

                            Thread.sleep((i + 1) * 1000);
                        } else {
                            break;
                        }
                    }

                    for (int i = 0; i < HBASE_MAX_RETRY_TIMES; i++) {
                        try {
                            hBaseClient.insertBatch(hBaseDataList);
                            break;
                        } catch (HBasePersistenceException e) {
                            if (i < HBASE_MAX_RETRY_TIMES - 1) {
                                LOG.error("Persisting aggregation data to HBase failed. Retry in " + (i + 1) + " second(s)");
                            } else {
                                LOG.error("The following aggregation data are dropped: %s", hBaseDataList);
                            }
                        }

                        Thread.sleep((i + 1) * 1000);
                    }

                    cacheManager.publish(redisCacheMap, REDIS_CHANNEL);

                    LOG.info("Persisting aggregation result done.");
                } catch (Exception e) {
                    LOG.error("Persistence of aggregated result failed.", e);
                } finally {
                    try {
                        Thread.sleep(PERIOD * 1000);
                    } catch (InterruptedException e) {
                        LOG.error("PersistThread was interrupted.", e);
                    }
                }
            }
        }
    }
}
