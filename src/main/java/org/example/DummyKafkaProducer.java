package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class DummyKafkaProducer {

    // 설정 변경 가능
    private static final String DEFAULT_BOOTSTRAP = "localhost:9092";
    private static final String DEFAULT_TOPIC = "dummy-topic";
    private static final long DEFAULT_INTERVAL_MS = 1000L; // 기본 1초

    public static void main(String[] args) {
        String bootstrap = args.length > 0 ? args[0] : DEFAULT_BOOTSTRAP;
        String topic = args.length > 1 ? args[1] : DEFAULT_TOPIC;
        long intervalMillis = args.length > 2 ? Long.parseLong(args[2]) : DEFAULT_INTERVAL_MS;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 필요시 조정

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicLong seq = new AtomicLong(0);

        DateTimeFormatter dtfSec = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        DateTimeFormatter dtfMillis = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

        Runnable task = () -> {
            long id = seq.incrementAndGet();
            LocalDateTime now = LocalDateTime.now();
            String timeKey = (intervalMillis < 1000) ? dtfMillis.format(now) : dtfSec.format(now);
            // 예시 포맷: uniqueId/timeKey/payload
            String uniqueId = "ID-" + (id % 10); // 예: 여러 ID 섞이게 하려면 변경
            String value = uniqueId + "|" + timeKey + "|payload-" + id;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, uniqueId, value);
            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    System.err.println("Send failed: " + ex.getMessage());
                } else {
                    System.out.printf("Sent key=%s partition=%d offset=%d time=%s%n",
                            uniqueId, metadata.partition(), metadata.offset(), timeKey);
                }
            });
        };

        // 스케줄 시작
        scheduler.scheduleAtFixedRate(task, 0, intervalMillis, TimeUnit.MILLISECONDS);

        // graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException ignored) { scheduler.shutdownNow(); }

            producer.flush();
            producer.close();
            System.out.println("Stopped.");
        }));

        System.out.printf("Started dummy producer -> bootstrap=%s topic=%s intervalMs=%d%n",
                bootstrap, topic, intervalMillis);
    }
}
