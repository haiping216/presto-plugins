package presto.event.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import io.airlift.log.Logger;

public class KafkaCallBack implements Callback {
    private static final Logger log = Logger.get(KafkaCallBack.class);

    private final long startTime;
    private final String message;

    public KafkaCallBack(long startTime, String message) {
        this.startTime = startTime;
        this.message = message;
    }

//    @Override
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
