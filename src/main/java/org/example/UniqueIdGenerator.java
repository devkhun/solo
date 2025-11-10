package org.example;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadLocalRandom;

public class UniqueIdGenerator {
    public static void main(String[] args) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        AtomicInteger uniqueId = new AtomicInteger(0);
        AtomicInteger repeatCount = new AtomicInteger(0);
        char[] suffixes = {'A', 'B', 'C'};

        scheduler.scheduleAtFixedRate(() -> {
            int id = uniqueId.get();

            // 3번 실행될 때마다 ID 증가
            if (repeatCount.incrementAndGet() > 3) {
                repeatCount.set(1);
                id = uniqueId.incrementAndGet();
            }

            // A, B, C 중 랜덤 선택
            char randomSuffix = suffixes[ThreadLocalRandom.current().nextInt(suffixes.length)];

            String finalId = id + String.valueOf(randomSuffix);

            System.out.println("Unique ID: " + finalId);

        }, 0, 100, TimeUnit.MILLISECONDS);
    }
}
