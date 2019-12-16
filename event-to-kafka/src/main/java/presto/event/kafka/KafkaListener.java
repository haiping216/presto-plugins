package presto.event.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import io.airlift.log.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class KafkaListener implements EventListener {

    private static final Logger log = Logger.get(KafkaListener.class);

    private KafkaProducer kafkaProducer;

    private String topic;

    public KafkaListener(Map<String, String> requiredConfig) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", requireNonNull(requiredConfig.get("event-listener.bootstrap.servers"), "event-listener.bootstrap.servers is null"));
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", StringSerializer.class.getName());
        kafkaProps.put("client.id", requireNonNull(requiredConfig.get("event-listener.client.id"), "event-listener.client.id is null"));
        this.kafkaProducer = new KafkaProducer<String, String>(kafkaProps);
        this.topic = requireNonNull(requiredConfig.get("event-listener.kafka-topic"), "event-listener.kafka-topic is null");
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        log.info("QUERY SQL : [ %s ]", queryCreatedEvent.getMetadata().getQuery());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        log.info("queryCompleted started");
        Map<String, Object> event = new HashMap<>();

        // QueryMetadata
        event.put("queryId", queryCompletedEvent.getMetadata().getQueryId());
        event.put("query", queryCompletedEvent.getMetadata().getQuery());
        event.put("uri", queryCompletedEvent.getMetadata().getUri());
        event.put("state", queryCompletedEvent.getMetadata().getQueryState());

        // QueryStatistics
        event.put("cpuTime", queryCompletedEvent.getStatistics().getCpuTime().toMillis());
        event.put("wallTime", queryCompletedEvent.getStatistics().getWallTime().toMillis());
        event.put("queuedTime", queryCompletedEvent.getStatistics().getQueuedTime().toMillis());
        if(queryCompletedEvent.getStatistics().getAnalysisTime().isPresent()) {
            event.put("analysisTime", queryCompletedEvent.getStatistics().getAnalysisTime().get().toMillis());
        }
        event.put("peakTotalNonRevocableMemoryBytes", queryCompletedEvent.getStatistics().getPeakTotalNonRevocableMemoryBytes());
        event.put("peakUserMemoryBytes", queryCompletedEvent.getStatistics().getPeakUserMemoryBytes());
        event.put("totalBytes", queryCompletedEvent.getStatistics().getTotalBytes());
        event.put("totalRows", queryCompletedEvent.getStatistics().getTotalRows());
        event.put("outputBytes", queryCompletedEvent.getStatistics().getOutputBytes());
        event.put("outputRows", queryCompletedEvent.getStatistics().getOutputRows());
        event.put("writtenOutputBytes", queryCompletedEvent.getStatistics().getWrittenOutputBytes());
        event.put("writtenOutputRows", queryCompletedEvent.getStatistics().getWrittenOutputRows());
        event.put("writtenIntermediateRows", queryCompletedEvent.getStatistics().getWrittenIntermediateBytes());
        event.put("cumulativeMemory", queryCompletedEvent.getStatistics().getCumulativeMemory());
        event.put("completedSplits", queryCompletedEvent.getStatistics().getCompletedSplits());

//        // QueryContext
//        event.put("user:", queryCompletedEvent.getContext().getUser());
//        if(queryCompletedEvent.getContext().getPrincipal().isPresent()) {
//            event.put("principal", queryCompletedEvent.getContext().getPrincipal());
//        }
//        if(queryCompletedEvent.getContext().getRemoteClientAddress().isPresent()) {
//            event.put("remoteClientAddress", queryCompletedEvent.getContext().getRemoteClientAddress());
//        }
//        if(queryCompletedEvent.getContext().getUserAgent().isPresent()) {
//            event.put("userAgent", queryCompletedEvent.getContext().getUserAgent());
//        }
//        if(queryCompletedEvent.getContext().getClientInfo().isPresent()) {
//            event.put("clientInfo", queryCompletedEvent.getContext().getClientInfo());
//        }
//        if(queryCompletedEvent.getContext().getSource().isPresent()) {
//            event.put("source", queryCompletedEvent.getContext().getSource());
//        }
//        if(queryCompletedEvent.getContext().getCatalog().isPresent()) {
//            event.put("catalog", queryCompletedEvent.getContext().getCatalog());
//        }
//        if(queryCompletedEvent.getContext().getSchema().isPresent()) {
//            event.put("schema", queryCompletedEvent.getContext().getSchema());
//        }
//        if(queryCompletedEvent.getContext().getResourceGroupId().isPresent()) {
//            event.put("resourceGroupId", queryCompletedEvent.getContext().getResourceGroupId());
//        }

        // QueryFailureInfo
        if(queryCompletedEvent.getFailureInfo().isPresent()) {
            QueryFailureInfo queryFailureInfo = queryCompletedEvent.getFailureInfo().get();
            event.put("errorCode", queryFailureInfo.getErrorCode());
            event.put("failureHost", queryFailureInfo.getFailureHost().orElse(""));
            event.put("failureMessage", queryFailureInfo.getFailureMessage().orElse(""));
            event.put("failureTask", queryFailureInfo.getFailureTask().orElse(""));
            event.put("failureType", queryFailureInfo.getFailureType().orElse(""));
            event.put("failuresJson", queryFailureInfo.getFailuresJson());
        }

        event.put("createTime", queryCompletedEvent.getCreateTime().toEpochMilli());
        event.put("endTime", queryCompletedEvent.getEndTime().toEpochMilli());
        event.put("executionStartTime", queryCompletedEvent.getExecutionStartTime().toEpochMilli());


        long startTime = System.currentTimeMillis();
        log.info(event.toString());
        String message = JSON.toJSONString(event, SerializerFeature.DisableCircularReferenceDetect);
        log.info(message);
        kafkaProducer.send(new ProducerRecord<String, String>(topic, message), new KafkaCallBack(startTime, event.toString()));
        log.info("queryCompleted ended");

    }
}