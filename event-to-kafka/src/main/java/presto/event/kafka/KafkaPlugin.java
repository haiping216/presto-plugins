package presto.event.kafka;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.google.common.collect.ImmutableList;

public class KafkaPlugin implements Plugin {

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories() {
        return ImmutableList.of(new KafkaFactory());
    }
}
