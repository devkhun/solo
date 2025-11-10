package org.example;

import java.util.*;
import java.util.concurrent.*;

public class FTLTestRunner {

    private static final int THREAD_COUNT = 10;
    private List<EventQueue> queues = new ArrayList<>();
    private List<Thread> workers = new ArrayList<>();
    private Subscriber sub;
    private FTLRealm ftl;

    // ---- 테스트용 Mock 클래스 (FTL 의존성 대체) ----
    static class FTLRealm {
        EventQueue createEventQueue() { return new EventQueue(); }
        Subscriber createSubscriber(String endpoint, String cm, Properties props) {
            return new Subscriber(endpoint);
        }
    }

    static class EventQueue {
        private BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
        void dispatch(long timeout) {
            try {
                Runnable r = tasks.poll(timeout, TimeUnit.MILLISECONDS);
                if (r != null) r.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        void enqueue(Runnable r) { tasks.add(r); }
        void addSubscriber(Subscriber sub, BiConsumer<List<Message>, EventQueue> handler) {
            // 단순 테스트에서는 수동 트리거
        }
    }

    static class Subscriber {
        String endpoint;
        Subscriber(String endpoint) { this.endpoint = endpoint; }
    }

    static class Message {
        private final Map<String, Object> map = new HashMap<>();
        void put(String key, Object val) { map.put(key, val); }
        String get(String key) { return (String) map.get(key); }
    }

    // ---- 실제 테스트 로직 ----
    public void init() {
        ftl = new FTLRealm();

        // 10개의 큐 + 워커 스레드 생성
        for (int i = 0; i < THREAD_COUNT; i++) {
            EventQueue q = ftl.createEventQueue();
            queues.add(q);

            Thread t = new Thread(() -> {
                while (true) q.dispatch(10);
            }, "worker-" + i);
            t.start();
            workers.add(t);
        }

        sub = ftl.createSubscriber("testEndpoint", "cm", new Properties());
    }

    public void subscribeAndTest() {
        // 메시지를 key 해시로 분배
        EventQueue mainQueue = queues.get(0);

        mainQueue.addSubscriber(sub, (msgs, qrecv) -> {
            for (Message msg : msgs) {
                String key = msg.get("key");
                int idx = Math.abs(key.hashCode()) % THREAD_COUNT;
                EventQueue target = queues.get(idx);
                target.enqueue(() -> processMessage(msg, idx));
            }
        });

        // ---- 테스트 입력 ----
        for (int i = 0; i < 50; i++) {
            Message msg = new Message();
            msg.put("key", "key" + (i % 5)); // 같은 키는 같은 스레드로 가야함
            msg.put("value", "data" + i);
            int idx = Math.abs(msg.get("key").hashCode()) % THREAD_COUNT;
            queues.get(idx).enqueue(() -> processMessage(msg, idx));
        }
    }

    private void processMessage(Message msg, int idx) {
        System.out.printf("[%s] Thread-%d processed: key=%s value=%s%n",
                Thread.currentThread().getName(), idx, msg.get("key"), msg.get("value"));
    }

    public static void main(String[] args) {
        FTLTestRunner runner = new FTLTestRunner();
        runner.init();
        runner.subscribeAndTest();
    }
}

