package presto.event.log;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.airlift.log.Logger;

import java.io.File;
import java.io.FileWriter;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LogListener implements EventListener
{
    private static final Logger log = Logger.get(LogListener.class);

    private final String logPath;
    private final String eventLogFileName;

    public LogListener(Map<String, String> requiredConfig) {
        logPath = requireNonNull(requiredConfig.get("event-listener.event-log-path"), "event-listener.event-log-path is null");
        eventLogFileName = requireNonNull(requiredConfig.get("event-listener.event-log-filename"), "event-listener.event-log-filename is null");
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        log.info("QUERY SQL queryCreated: [ %s ]", queryCreatedEvent.getMetadata().getQuery());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        log.info("QUERY SQL queryCompleted: [ %s ]", queryCompletedEvent.getMetadata().getQuery());
        LogRecord record = buildLogRecord(queryCompletedEvent);

        Gson obj = new GsonBuilder().disableHtmlEscaping().create();
        try (FileWriter file = new FileWriter(logPath + File.separator + eventLogFileName, true)) {
            file.write(obj.toJson(record));
            file.write(System.lineSeparator());
        }
        catch (Exception e) {
            log.error("Error writing event log to file. ErrorMessage:" + e.getMessage());
            log.error("EventLog write failed:" + obj.toJson(record));
        }
    }

    LogRecord buildLogRecord(QueryCompletedEvent queryCompletedEvent) {
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSS").withZone(ZoneId.systemDefault());

        LogRecord record = new LogRecord();
        record.setQueryId(queryCompletedEvent.getMetadata().getQueryId());

        //SQL Query Text
        record.setQuery(queryCompletedEvent.getMetadata().getQuery());
        record.setUri(queryCompletedEvent.getMetadata().getUri().toString());
        record.setState(queryCompletedEvent.getMetadata().getQueryState());

        record.setCpuTime(queryCompletedEvent.getStatistics().getCpuTime().toMillis() / 1000.0);
        record.setWallTime(queryCompletedEvent.getStatistics().getWallTime().toMillis() / 1000.0);
        record.setQueuedTime(queryCompletedEvent.getStatistics().getQueuedTime().toMillis() / 1000.0);
        record.setPeakMemoryBytes(queryCompletedEvent.getStatistics().getPeakUserMemoryBytes());
        record.setTotalBytes(queryCompletedEvent.getStatistics().getTotalBytes());
        record.setTotalRows(queryCompletedEvent.getStatistics().getTotalRows());

        record.setCreateTime(formatter.format(queryCompletedEvent.getCreateTime()));
        record.setExecutionStartTime(formatter.format(queryCompletedEvent.getExecutionStartTime()));
        record.setEndTime(formatter.format(queryCompletedEvent.getEndTime()));

        record.setRemoteClientAddress(queryCompletedEvent.getContext().getRemoteClientAddress().orElse(""));
        record.setClientUser(queryCompletedEvent.getContext().getUser());
        record.setUserAgent(queryCompletedEvent.getContext().getUserAgent().orElse(""));
        record.setSource(queryCompletedEvent.getContext().getSource().orElse(""));
        return record;
    }
}
