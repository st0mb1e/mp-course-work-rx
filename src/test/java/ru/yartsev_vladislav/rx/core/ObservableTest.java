package ru.yartsev_vladislav.rx.core;

import org.junit.jupiter.api.Test;
import ru.yartsev_vladislav.rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class ObservableTest {

    @Test
    void shouldEmitAllItemsFromArray() {
        List<Integer> result = new ArrayList<>();

        Observable.from(1, 2, 3)
                .subscribe(result::add);

        assertEquals(List.of(1, 2, 3), result);
    }

    @Test
    void shouldEmitAllItemsFromIterable() {
        List<Integer> result = new ArrayList<>();

        Observable.create(List.of(4, 5, 6))
                .subscribe(result::add);

        assertEquals(List.of(4, 5, 6), result);
    }

    @Test
    void shouldCallOnComplete() {
        boolean[] completed = { false };

        Observable.from(1)
                .subscribe(
                        item -> {},
                        Throwable::printStackTrace,
                        () -> completed[0] = true
                );

        assertTrue(completed[0]);
    }

    @Test
    void shouldCallOnError() {
        RuntimeException exception = new RuntimeException("boom");
        Throwable[] error = new Throwable[1];

        Observable.<Integer>create(observer -> {
            observer.onError(exception);
        }).subscribe(
                item -> {},
                e -> error[0] = e
        );

        assertEquals(exception, error[0]);
    }

    @Test
    void shouldNotEmitAfterError() {
        List<Integer> result = new ArrayList<>();

        Observable.<Integer>create(observer -> {
            observer.onNext(1);
            observer.onError(new RuntimeException("fail"));
            observer.onNext(2); // не должен дойти
        }).subscribe(
                result::add,
                e -> {}
        );

        assertEquals(List.of(1), result);
    }

    @Test
    void shouldSupportMapOperator() {
        List<Integer> result = new ArrayList<>();

        Observable.from(1, 2, 3)
                .map(x -> x * 2)
                .subscribe(result::add);

        assertEquals(List.of(2, 4, 6), result);
    }

    @Test
    void shouldSupportFilterOperator() {
        List<Integer> result = new ArrayList<>();

        Observable.from(1, 2, 3, 4)
                .filter(x -> x % 2 == 0)
                .subscribe(result::add);

        assertEquals(List.of(2, 4), result);
    }

    @Test
    void shouldSupportFlatMapOperator() {
        List<Integer> result = new ArrayList<>();

        Observable.from(1, 2)
                .flatMap(x -> Observable.from(x, x * 10))
                .subscribe(result::add);

        assertEquals(List.of(1, 10, 2, 20), result);
    }

    @Test
    void flatMap_shouldPropagateError() {
        Throwable[] error = new Throwable[1];

        Observable.from(1, 2)
                .flatMap(x -> {
                    if (x == 2) throw new RuntimeException("boom");
                    return Observable.from(x);
                })
                .subscribe(
                        item -> {},
                        e -> error[0] = e
                );

        assertNotNull(error[0]);
        assertEquals("boom", error[0].getMessage());
    }

    @Test
    void shouldWorkWithSubscribeOverloads() {
        List<Integer> result = new ArrayList<>();
        boolean[] completed = { false };

        Observable.from(1, 2)
                .subscribe(
                        result::add,
                        Throwable::printStackTrace,
                        () -> completed[0] = true
                );

        assertEquals(List.of(1, 2), result);
        assertTrue(completed[0]);
    }

    @Test
    void shouldRunOnDifferentThreadWithObserveOn() throws InterruptedException {
        String mainThread = Thread.currentThread().getName();
        String[] threadName = new String[1];

        CountDownLatch latch = new CountDownLatch(1);

        Observable.from(1)
                .observeOn(Schedulers.single())
                .subscribe(
                        item -> threadName[0] = Thread.currentThread().getName(),
                        Throwable::printStackTrace,
                        latch::countDown
                );

        latch.await();

        assertNotNull(threadName[0]);
        assertNotEquals(mainThread, threadName[0]);
    }

    @Test
    void shouldRunOnDifferentThreadWithSubscribeOn() throws InterruptedException {
        String mainThread = Thread.currentThread().getName();
        String[] threadName = new String[1];

        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(observer -> {
                    threadName[0] = Thread.currentThread().getName();
                    observer.onNext(1);
                    observer.onComplete();
                })
                .subscribeOn(Schedulers.single())
                .subscribe(
                        item -> {},
                        Throwable::printStackTrace,
                        latch::countDown
                );

        latch.await();

        assertNotNull(threadName[0]);
        assertNotEquals(mainThread, threadName[0]);
    }
}