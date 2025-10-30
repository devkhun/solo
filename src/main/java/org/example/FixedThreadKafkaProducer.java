package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class FixedThreadKafkaProducer {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "dummy-topic";
    private static final int THREAD_COUNT = 10;         // 총 스레드 개수
    private static final long INTERVAL_MS = 500L;       // 발행 간격 (밀리초)
    private static final long DURATION_SEC = 30L;       // 전체 실행 시간 (초)
    private static final int TOTAL_DATA = 100;          // 전체 더미 데이터 개수

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        // 각 스레드별 큐 (자신의 데이터만 처리)
        List<BlockingQueue<Integer>> threadQueues = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            threadQueues.add(new LinkedBlockingQueue<>());
        }

        // 더미 데이터 생성 (데이터ID 기준으로 스레드 할당)
        for (int i = 1; i <= TOTAL_DATA; i++) {
            int assignedThread = i % THREAD_COUNT; // 데이터 i는 특정 스레드 전용
            threadQueues.get(assignedThread).add(i);
        }

        // 각 스레드 실행
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    Integer dataId = threadQueues.get(threadId).poll();
                    if (dataId == null) break; // 처리할 데이터 없으면 종료

                    LocalDateTime now = LocalDateTime.now();
                    String timeKey = fmt.format(now);
                    String eqpId = "EQP-" + dataId;
                    String value = "Thread=" + threadId + " | Data=" + dataId + " | Time=" + timeKey;

                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, eqpId, value);
                    producer.send(record, (meta, ex) -> {
                        if (ex == null) {
                            System.out.printf("[T-%02d] Sent partition=%d offset=%d value=%s%n",
                                    threadId, meta.partition(), meta.offset(), value);
                        } else {
                            System.err.printf("[T-%02d] Error: %s%n", threadId, ex.getMessage());
                        }
                    });

                    try {
                        Thread.sleep(INTERVAL_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        // 전체 실행 시간 후 종료
        ScheduledExecutorService stopper = Executors.newSingleThreadScheduledExecutor();
        stopper.schedule(() -> {
            System.out.println("== 종료 ==");
            executor.shutdownNow();
            producer.flush();
            producer.close();
            stopper.shutdown();
        }, DURATION_SEC, TimeUnit.SECONDS);
    }
}

