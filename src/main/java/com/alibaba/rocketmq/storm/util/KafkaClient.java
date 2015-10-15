package com.alibaba.rocketmq.storm.util;

import com.alibaba.fastjson.JSONObject;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.util.Properties;
import java.util.ResourceBundle;

/**
 * KafkaClient Created with mythopoet.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/10/15 下午2:54
 * @desc
 */
public class KafkaClient {

    private static KafkaClient kafkaClient;

    private static Producer<String, String> producer;

    private static String topic;

    private static Logger LOG = LoggerFactory.getLogger(KafkaClient.class);

    static {
        Properties props = new Properties();
        try {
            ResourceBundle bundle = ResourceBundle.getBundle("kafka");
            if ( bundle == null )
                throw new IllegalArgumentException("[kafka.properties] is not found");
            props.put("metadata.broker.list", bundle.getString("kafka.broker.list"));
            //        props.put("serializer.class", bundle.getString("kafka.serializer.class"));
            // key.serializer.class默认为serializer.class
            //        props.put("key.serializer.class", bundle.getString("kafka.key.serializer.class"));
            props.put("request.required.acks", Short.valueOf(bundle.getString("kafka.producer.ack")));
            props.put("request.timeout.ms", Integer.valueOf(bundle.getString("kafka.producer.request.timeout.ms")));
            props.put("producer.type", bundle.getString("kafka.producer.type"));
            props.put("queue.buffering.max.ms", Integer.valueOf(bundle.getString("kafka.producer.queue.buffering.max.ms")));
            props.put("queue.buffering.max.messages", Integer.valueOf(bundle.getString("kafka.producer.queue.buffering.max.messages")));


            topic = bundle.getString("kafka.topic");
            LOG.warn("KafkaConfig : " + JSONObject.toJSONString(props) + "---kafka_topic=" + topic + "---");
            ProducerConfig config = new ProducerConfig(props);
            // 创建producer
            producer = new Producer<String, String>(config);
        } catch ( Exception e ) {
            LOG.error("KafkaClient static init Error:props=" + JSONObject.toJSONString(props), e);
        }

    }

    public static KafkaClient getInstance() {
        if ( kafkaClient == null ) {
            kafkaClient = new KafkaClient();
        }
        return kafkaClient;
    }

    public void send(String message) {
        try {
            KeyedMessage<String, String> data = new KeyedMessage<>(topic, message);
            producer.send(data);
        } catch ( Exception e ) {
            LOG.error("KafkaClient.send error:message=" + message, e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String log = "0.174-_-186.2.136.129-_-global.ymtracking.com-_-10.1.10.11:8080-_-302-_-23/Sep/2015:06:05:02 +0000-_-GET\n"
                + "        /trace?offer_id=104259&aff_id=13468&aff_sub=13894046979 HTTP/1.1-_-302-_-541-_---_-Mozilla/5.0 (Linux; U; Android 4.2.2; es-es; " +
                "Bmobile_AX620\n" + "        Build/JDQ39) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30-_---_-0.174";

        String log1 = "testsetstsetststs";
        for ( int i = 0; i < 200000; i++ ) {
            getInstance().send(log1);
            if ( i%10000 == 0 ){
                Thread.sleep(2000);
            }
            System.out.println("suc--" + i);
        }

    }

}
