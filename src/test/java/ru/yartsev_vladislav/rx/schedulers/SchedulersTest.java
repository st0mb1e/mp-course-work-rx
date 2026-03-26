package ru.yartsev_vladislav.rx.schedulers;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SchedulersTest {

    @Test
    void ioScheduler_shouldRunTasksOnDifferentThreads() throws InterruptedException {
        IOThreadScheduler scheduler = new IOThreadScheduler();

        CountDownLatch latch = new CountDownLatch(5);
        Set<String> threadNames = Collections.synchronizedSet(new HashSet<>());

        for (int i = 0; i < 5; i++) {
            scheduler.execute(() -> {
                threadNames.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }

        latch.await();

        // ожидаем, что потоков больше одного
        assertTrue(threadNames.size() > 1);
    }

    @Test
    void computationScheduler_shouldRunTasksConcurrently() throws InterruptedException {
        ComputationScheduler scheduler = new ComputationScheduler();

        CountDownLatch latch = new CountDownLatch(5);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            scheduler.execute(() -> {
                counter.incrementAndGet();
                latch.countDown();
            });
        }

        latch.await();

        assertEquals(5, counter.get());
    }

    @Test
    void singleThreadScheduler_shouldUseSingleThread() throws InterruptedException {
        SingleThreadScheduler scheduler = new SingleThreadScheduler();

        CountDownLatch latch = new CountDownLatch(5);
        Set<String> threadNames = Collections.synchronizedSet(new HashSet<>());

        for (int i = 0; i < 5; i++) {
            scheduler.execute(() -> {
                threadNames.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }

        latch.await();

        // должен быть только один поток
        assertEquals(1, threadNames.size());
    }

    @Test
    void singleThreadScheduler_shouldExecuteTasksSequentially() throws InterruptedException {
        SingleThreadScheduler scheduler = new SingleThreadScheduler();

        CountDownLatch latch = new CountDownLatch(3);
        StringBuilder result = new StringBuilder();

        scheduler.execute(() -> {
            result.append("A");
            latch.countDown();
        });

        scheduler.execute(() -> {
            result.append("B");
            latch.countDown();
        });

        scheduler.execute(() -> {
            result.append("C");
            latch.countDown();
        });

        latch.await();

        assertEquals("ABC", result.toString());
    }

    @Test
    void scheduler_shouldNotLoseTasksUnderLoad() throws InterruptedException {
        IOThreadScheduler scheduler = new IOThreadScheduler();

        int taskCount = 100;
        CountDownLatch latch = new CountDownLatch(taskCount);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < taskCount; i++) {
            scheduler.execute(() -> {
                counter.incrementAndGet();
                latch.countDown();
            });
        }

        latch.await();

        assertEquals(taskCount, counter.get());
    }
}