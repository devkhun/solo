package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

public class QueueBasedKafkaPublisher {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "dummy-topic";
    private static final int THREAD_COUNT = 10;
    private static final long INTERVAL_MS = 500;
    private static final long DURATION_SEC = 20;
    private static final int TOTAL_DATA = 100;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        // 공용 큐
        BlockingQueue<Integer> sharedQueue = new LinkedBlockingQueue<>();

        // 스레드별 전용 큐
        List<BlockingQueue<Integer>> workerQueues = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            workerQueues.add(new LinkedBlockingQueue<>());
        }

        ExecutorService workers = Executors.newFixedThreadPool(THREAD_COUNT);

        // 각 워커 스레드 실행
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            workers.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Integer dataId = workerQueues.get(threadId).poll(1, TimeUnit.SECONDS);
                        if (dataId == null) continue;

                        LocalDateTime now = LocalDateTime.now();
                        String timeKey = fmt.format(now);
                        String eqpId = "EQP-" + dataId;
                        String value = "Thread=" + threadId + " | Data=" + dataId + " | Time=" + timeKey;

                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, eqpId, value);
                        producer.send(record);

                        System.out.printf("[T-%02d] Sent %s%n", threadId, value);
                        Thread.sleep(INTERVAL_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        // 라우터 스레드: 데이터ID → 담당 스레드 큐로 분배
        Thread router = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Integer id = sharedQueue.poll(1, TimeUnit.SECONDS);
                    if (id == null) continue;

                    int threadId = id % THREAD_COUNT;
                    workerQueues.get(threadId).put(id);
                }
            } catch (InterruptedException ignored) {}
        });
        router.start();

        // 더미 데이터 생성
        for (int i = 1; i <= TOTAL_DATA; i++) {
            sharedQueue.add(i);
        }

        // 종료 타이머
        ScheduledExecutorService stopper = Executors.newSingleThreadScheduledExecutor();
        stopper.schedule(() -> {
            System.out.println("== 종료 ==");
            router.interrupt();
            workers.shutdownNow();
            producer.flush();
            producer.close();
            stopper.shutdown();
        }, DURATION_SEC, TimeUnit.SECONDS);
    }
}

