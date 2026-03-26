package ru.yartsev_vladislav.rx.internal;

import org.junit.jupiter.api.Test;
import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class ObserveOnObservableTest {

    @Test
    void observeOn_shouldSwitchThread() throws InterruptedException {
        String mainThread = Thread.currentThread().getName();
        CountDownLatch latch = new CountDownLatch(1);
        String[] threadName = new String[1];

        Observable.from(1)
                .observeOn(Schedulers.single())
                .subscribe(
                        item -> threadName[0] = Thread.currentThread().getName(),
                        Throwable::printStackTrace,
                        latch::countDown
                );

        latch.await();

        assertNotNull(threadName[0]);
        assertNotEquals(mainThread, threadName[0], "observeOn должен использовать другой поток");
    }

    @Test
    void observeOn_shouldEmitAllValues() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> result = new ArrayList<>();

        Observable.from(1, 2, 3)
                .observeOn(Schedulers.single())
                .subscribe(
                        result::add,
                        Throwable::printStackTrace,
                        latch::countDown
                );

        latch.await();

        assertEquals(List.of(1, 2, 3), result, "Все значения должны пройти через observeOn");
    }

    @Test
    void observeOn_shouldPreserveOrder() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> result = new ArrayList<>();

        Observable.from(10, 20, 30, 40)
                .observeOn(Schedulers.single())
                .subscribe(
                        result::add,
                        Throwable::printStackTrace,
                        latch::countDown
                );

        latch.await();

        assertEquals(List.of(10, 20, 30, 40), result, "observeOn должен сохранять порядок элементов");
    }

    @Test
    void observeOn_shouldPropagateError() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        RuntimeException exception = new RuntimeException("fail");
        Throwable[] error = new Throwable[1];

        Observable<Integer> source = Observable.create(observer -> observer.onError(exception));

        source.observeOn(Schedulers.single())
                .subscribe(
                        item -> {},
                        e -> {
                            error[0] = e;
                            latch.countDown();
                        }
                );

        latch.await();

        assertEquals(exception, error[0], "observeOn должен пробрасывать ошибки");
    }

    @Test
    void observeOn_shouldCallOnComplete() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        boolean[] completed = { false };

        Observable.from(1)
                .observeOn(Schedulers.single())
                .subscribe(
                        item -> {},
                        Throwable::printStackTrace,
                        () -> {
                            completed[0] = true;
                            latch.countDown();
                        }
                );

        latch.await();

        assertTrue(completed[0], "observeOn должен вызывать onComplete");
    }
}
