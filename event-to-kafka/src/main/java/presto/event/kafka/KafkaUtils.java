package presto.event.kafka;

import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import io.airlift.log.Logger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class KafkaUtils {

    private static final Logger log = Logger.get(KafkaListener.class);

    private String commonTopic;

    private String prestoTopic;

    private KafkaProducer kafkaProducer;

    public KafkaUtils(Map<String, String> requiredConfig) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", requireNonNull(requiredConfig.get("event-listener.bootstrap.servers"), "event-listener.bootstrap.servers is null"));
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", StringSerializer.class.getName());
        kafkaProps.put("client.id", requireNonNull(requiredConfig.get("event-listener.client.id"), "event-listener.client.id is null"));
        this.kafkaProducer = new KafkaProducer<String, String>(kafkaProps);
        this.commonTopic = requireNonNull(requiredConfig.get("event-listener.kafka-topic1"), "event-listener.kafka-topic1 is null");
        this.prestoTopic = requireNonNull(requiredConfig.get("event-listener.kafka-topic2"), "event-listener.kafka-topic2 is null");
    }

    public void SendToCommonTopic(QueryCompletedEvent queryCompletedEvent) {

        log.info("SendToCommonTopic start");
        JSONObject msgJson = new JSONObject();
        msgJson.put("engine", "presto");
        msgJson.put("commit_type", "presto-sql");
        msgJson.put("commit_ip", "");
        msgJson.put("commit_port", "");
        msgJson.put("commit_username", queryCompletedEvent.getContext().getUser());
        msgJson.put("commit_time", queryCompletedEvent.getCreateTime().toEpochMilli());
        msgJson.put("sql", queryCompletedEvent.getMetadata().getQuery());

        String message = msgJson.toJSONString();
        log.info(message);
        ProducerRecord producerRecord = new ProducerRecord<String, String>(commonTopic, message);

        long startTime = System.currentTimeMillis();
        KafkaCallBack kafkaCallBack = new KafkaCallBack(startTime, message);
        kafkaProducer.send(producerRecord, kafkaCallBack);
        log.info("SendToCommonTopic end");
    }

    public void SendToPrestoTopic(QueryCompletedEvent queryCompletedEvent) {
        log.info("SendToPrestoTopic start");
        JSONObject msgJson = new JSONObject();
        msgJson.put("query_id", queryCompletedEvent.getMetadata().getQueryId());
        msgJson.put("state", queryCompletedEvent.getMetadata().getQueryState());
        msgJson.put("user", queryCompletedEvent.getContext().getUser());
        msgJson.put("source", queryCompletedEvent.getContext().getSource());
        msgJson.put("query", queryCompletedEvent.getMetadata().getQuery());
        msgJson.put("resource_group_id", queryCompletedEvent.getContext().getResourceGroupId());
        msgJson.put("queued_time_ms", queryCompletedEvent.getStatistics().getQueuedTime().toMillis());
        msgJson.put("analysis_time_ms", queryCompletedEvent.getStatistics().getAnalysisTime().get().toMillis());
        msgJson.put("created", queryCompletedEvent.getCreateTime().toEpochMilli());
        msgJson.put("started", queryCompletedEvent.getExecutionStartTime());
        msgJson.put("last_heartbeat", queryCompletedEvent.getEndTime());
        msgJson.put("end", queryCompletedEvent.getEndTime());

        String message = msgJson.toJSONString();
        log.info(message);
        ProducerRecord producerRecord = new ProducerRecord<String, String>(commonTopic, message);

        long startTime = System.currentTimeMillis();
        KafkaCallBack kafkaCallBack = new KafkaCallBack(startTime, message);
        kafkaProducer.send(producerRecord, kafkaCallBack);
        log.info("SendToPrestoTopic end");
    }
}

class KafkaCallBack implements Callback {
    private static final Logger log = Logger.get(KafkaCallBack.class);

    private final long startTime;
    private final String message;

    public KafkaCallBack(long startTime, String message) {
        this.startTime = startTime;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        log.info("onCompletion start");
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            log.info("message(" + message + ") sent to partition("
                    + metadata.partition() + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            System.out.println("message(" + message + ") sent to partition("
                    + metadata.partition() + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
        log.info("onCompletion end");
    }
}
