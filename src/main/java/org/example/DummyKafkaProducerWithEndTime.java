package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class DummyKafkaProducerWithEndTime {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "dummy-topic";
    private static final long INTERVAL_MS = 1000L; // 발행 간격 (1초)
    private static final LocalDateTime END_TIME = LocalDateTime.now().plusMinutes(1); // 1분간만 실행

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicLong seq = new AtomicLong(0);
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        Runnable task = () -> {
            if (LocalDateTime.now().isAfter(END_TIME)) {
                System.out.println("End time reached. Stopping producer.");
                scheduler.shutdown();
                producer.flush();
                producer.close();
                return;
            }

            long id = seq.incrementAndGet();
            LocalDateTime now = LocalDateTime.now();
            String timeKey = fmt.format(now);
            String uniqueId = "EQP-" + (id % 5); // 장비ID 랜덤패턴 예시
            String value = uniqueId + "|" + timeKey + "|payload-" + id;

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, uniqueId, value);
            producer.send(record, (meta, ex) -> {
                if (ex != null) {
                    System.err.println("Send failed: " + ex.getMessage());
                } else {
                    System.out.printf("Sent [%s] partition=%d offset=%d%n",
                            value, meta.partition(), meta.offset());
                }
            });
        };

        scheduler.scheduleAtFixedRate(task, 0, INTERVAL_MS, TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown triggered.");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) scheduler.shutdownNow();
            } catch (InterruptedException ignored) { scheduler.shutdownNow(); }
            producer.flush();
            producer.close();
        }));

        System.out.printf("Started producer. Ends at %s%n", END_TIME);
    }
}
