package presto.event.kafka;

import com.facebook.presto.spi.eventlistener.*;
import io.airlift.log.Logger;
import java.util.Map;

public class KafkaListener implements EventListener {

    private static final Logger log = Logger.get(KafkaListener.class);

    private KafkaUtils kafkaUtils;

    public KafkaListener(Map<String, String> requiredConfig) {
        kafkaUtils = new KafkaUtils(requiredConfig);
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
        kafkaUtils.SendToCommonTopic(queryCompletedEvent);
//        kafkaUtils.SendToPrestoTopic(queryCompletedEvent);
        log.info("queryCompleted ended");
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
        log.info("splitCompleted:" + splitCompletedEvent.getStatistics().toString());
    }
}