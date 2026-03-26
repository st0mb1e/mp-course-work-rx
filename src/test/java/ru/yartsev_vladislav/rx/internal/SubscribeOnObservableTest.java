package ru.yartsev_vladislav.rx.internal;

import org.junit.jupiter.api.Test;
import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.schedulers.Schedulers;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class SubscribeOnObservableTest {

    @Test
    void subscribeOn_shouldRunOnDifferentThread() throws InterruptedException {
        String mainThread = Thread.currentThread().getName();

        CountDownLatch latch = new CountDownLatch(1);
        String[] threadName = new String[1];

        Observable<Integer> source = Observable.create(observer -> {
            threadName[0] = Thread.currentThread().getName();
            observer.onNext(1);
            observer.onComplete();
        });

        source
                .subscribeOn(Schedulers.io())
                .subscribe(
                        item -> {},
                        Throwable::printStackTrace,
                        latch::countDown
                );

        latch.await();

        assertNotNull(threadName[0]);
        assertNotEquals(mainThread, threadName[0]);
    }

    @Test
    void subscribeOn_shouldEmitValues() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        int[] result = {0};

        Observable<Integer> source = Observable.create(observer -> {
            observer.onNext(42);
            observer.onComplete();
        });

        source
                .subscribeOn(Schedulers.io())
                .subscribe(
                        item -> result[0] = item,
                        Throwable::printStackTrace,
                        latch::countDown
                );

        latch.await();

        assertEquals(42, result[0]);
    }

    @Test
    void subscribeOn_shouldPropagateError() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        RuntimeException exception = new RuntimeException("boom");
        Throwable[] error = new Throwable[1];

        Observable<Integer> source = Observable.create(observer -> {
            observer.onError(exception);
        });

        source
                .subscribeOn(Schedulers.io())
                .subscribe(
                        item -> {},
                        e -> {
                            error[0] = e;
                            latch.countDown();
                        }
                );

        latch.await();

        assertEquals(exception, error[0]);
    }
}