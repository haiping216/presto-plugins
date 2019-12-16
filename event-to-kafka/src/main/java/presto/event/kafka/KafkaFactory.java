package presto.event.kafka;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import io.airlift.log.Logger;
import java.util.Map;
import static java.util.Objects.requireNonNull;

public class KafkaFactory implements EventListenerFactory {
    private static final Logger log = Logger.get(KafkaFactory.class);

    public String getName() {
        return "event-to-kafka";
    }

    public EventListener create(Map<String, String> requiredConfig) {
        log.info("=========create==========");
        requireNonNull(requiredConfig, "requiredConfig is null");
        return new KafkaListener(requiredConfig);
    }
}
