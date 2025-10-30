package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.*;

public class MultiThreadStandardizeAndPublish {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "standard-topic";

    private static final int THREAD_COUNT = 10;
    private static final int QUEUE_CAPACITY = 1000;

    // 표준화 완료된 데이터를 담을 큐
    private static final BlockingQueue<String> standardizedQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    public static void main(String[] args) {
        ExecutorService standardizerPool = Executors.newFixedThreadPool(THREAD_COUNT);
        ExecutorService publisherPool = Executors.newSingleThreadExecutor();

        // Kafka Producer 설정
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ① 표준화 스레드 (데이터 처리)
        for (int i = 0; i < THREAD_COUNT; i++) {
            int threadId = i;
            standardizerPool.submit(() -> {
                try {
                    while (true) {
                        // 실제 환경에서는 외부 입력에서 데이터 읽기
                        String rawData = "rawData-" + threadId + "-" + System.nanoTime();

                        // 데이터 표준화 로직
                        String standardized = standardizeData(rawData);

                        // 표준화된 데이터를 큐에 추가
                        standardizedQueue.put(standardized);
                        System.out.printf("[Thread-%d] standardized and queued: %s%n", threadId, standardized);

                        Thread.sleep(300); // 표준화 처리 속도 조절용
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // ② 발행 스레드 (카프카 전송)
        publisherPool.submit(() -> {
            try {
                while (true) {
                    String msg = standardizedQueue.take(); // 표준화 완료된 데이터 가져오기
                    producer.send(new ProducerRecord<>(TOPIC, extractKey(msg), msg),
                            (meta, ex) -> {
                                if (ex != null)
                                    System.err.println("Kafka send failed: " + ex.getMessage());
                                else
                                    System.out.printf("[Kafka] sent %s (partition=%d offset=%d)%n",
                                            msg, meta.partition(), meta.offset());
                            });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            standardizerPool.shutdownNow();
            publisherPool.shutdownNow();
            producer.flush();
            producer.close();
        }));
    }

    // 데이터 표준화 메서드 (예시)
    private static String standardizeData(String raw) {
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return raw.replace("rawData", "stdData") + "|" + fmt.format(LocalDateTime.now());
    }

    // Kafka 파티션 키 추출 (예시)
    private static String extractKey(String msg) {
        // stdData-<threadId>-<nano>|<timestamp> → threadId 기준 키 추출
        String[] parts = msg.split("-");
        return parts.length > 1 ? parts[1] : "default";
    }
}

