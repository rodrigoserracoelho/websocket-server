package io.surisoft.websocket.handler;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.SchedulingAwareRunnable;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.Duration.ofMillis;

public class ConsumerRunner implements SchedulingAwareRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunner.class);

    private long pollTimeout = 100L;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private Consumer<String, String> consumer;
    private ConsumerRecordProcessor<String, String> processor;

    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setProcessor(ConsumerRecordProcessor<String, String> processor) {
        this.processor = processor;
    }

    public ConsumerRecordProcessor<String, String> getProcessor() {
        return processor;
    }

    public void shutdown() {
        LOGGER.info("Shutting down consumer: {}", consumer);
        closed.set(true);
        consumer.wakeup();
    }

    @Override
    public boolean isLongLived() {
        return true;
    }

    @Override
    public void run() {
        try {
            LOGGER.info("Running consumer: {}", consumer);
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(ofMillis(pollTimeout));
                if (records != null && !records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.debug("Processing record: {}", record);
                        processor.process(record);
                    }
                }
            }
        } catch (WakeupException e) {
            LOGGER.info("Wakeup consumer: {}", consumer);
            if (!closed.get()) {
                LOGGER.error("Wakeup exception without close.", e);
                throw new RuntimeException("Wakeup exception without close.", e);
            } else {
                LOGGER.info("Wakeup closed consumer: " + consumer, e);
            }
        } finally {
            LOGGER.debug("Closing consumer: {}", consumer);
            consumer.close();
        }
    }
}