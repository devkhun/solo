package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;

public class MultiThreadKafkaDummyProducer {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "dummy-topic";
    private static final int THREAD_COUNT = 5;         // 동시에 메시지 보낼 스레드 수
    private static final long INTERVAL_MS = 1000L;     // 각 스레드의 발행 간격
    private static final long DURATION_SEC = 30L;      // 전체 실행 시간 (초 단위)

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        Random rand = new Random();

        Runnable worker = () -> {
            long seq = 0;
            while (!Thread.currentThread().isInterrupted()) {
                seq++;
                LocalDateTime now = LocalDateTime.now();
                String timeKey = fmt.format(now);
                String eqpId = "EQP-" + rand.nextInt(10);
                String eventCd = "EV-" + (seq % 3 + 1);
                String value = eqpId + "|" + eventCd + "|" + timeKey + "|dummy=" + seq;

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, eqpId, value);
                producer.send(record, (meta, ex) -> {
                    if (ex != null) {
                        System.err.println("[" + Thread.currentThread().getName() + "] Error: " + ex.getMessage());
                    } else {
                        System.out.printf("[%s] Sent partition=%d offset=%d value=%s%n",
                                Thread.currentThread().getName(), meta.partition(), meta.offset(), value);
                    }
                });

                try {
                    Thread.sleep(INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(worker);
        }

        ScheduledExecutorService stopper = Executors.newSingleThreadScheduledExecutor();
        stopper.schedule(() -> {
            System.out.println("== Stopping all threads ==");
            executor.shutdownNow();
            producer.flush();
            producer.close();
            stopper.shutdown();
        }, DURATION_SEC, TimeUnit.SECONDS);

        System.out.printf("Started %d producer threads for %d seconds (interval %d ms)%n",
                THREAD_COUNT, DURATION_SEC, INTERVAL_MS);
    }
}
