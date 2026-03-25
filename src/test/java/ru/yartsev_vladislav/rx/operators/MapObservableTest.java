package ru.yartsev_vladislav.rx.operators;

import org.junit.jupiter.api.Test;
import ru.yartsev_vladislav.rx.core.Observable;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MapObservableTest {

    @Test
    void map_shouldTransformItems() {
        List<Integer> result = new ArrayList<>();

        Observable<Integer> source = Observable.from(1, 2, 3);

        Observable<Integer> mapped = new MapObservable<>(
                source,
                x -> x * 2
        );

        mapped.subscribe(result::add);

        assertEquals(List.of(2, 4, 6), result);
    }

    @Test
    void map_shouldCallOnComplete() {
        boolean[] completed = {false};

        Observable<Integer> source = Observable.from(1, 2, 3);

        Observable<Integer> mapped = new MapObservable<>(
                source,
                x -> x
        );

        mapped.subscribe(
                item -> {},
                error -> fail(),
                () -> completed[0] = true
        );

        assertTrue(completed[0]);
    }

    @Test
    void map_shouldPropagateErrorFromSource() {
        RuntimeException exception = new RuntimeException("boom");

        Observable<Integer> source = Observable.create(observer -> {
            observer.onNext(1);
            observer.onError(exception);
        });

        List<Integer> result = new ArrayList<>();
        Throwable[] errorHolder = {null};

        Observable<Integer> mapped = new MapObservable<>(
                source,
                x -> x * 2
        );

        mapped.subscribe(
                result::add,
                e -> errorHolder[0] = e
        );

        assertEquals(List.of(2), result);
        assertEquals(exception, errorHolder[0]);
    }

    @Test
    void map_shouldHandleExceptionInFunction() {
        RuntimeException exception = new RuntimeException("mapper error");

        Observable<Integer> source = Observable.create(List.of(1, 2, 3));

        Throwable[] errorHolder = {null};

        Observable<Integer> mapped = new MapObservable<>(
                source,
                x -> {
                    if (x == 2) throw exception;
                    return x;
                }
        );

        mapped.subscribe(
                item -> {},
                e -> errorHolder[0] = e
        );

        assertEquals(exception, errorHolder[0]);
    }

    @Test
    void map_shouldWorkInChain() {
        List<Integer> result = new ArrayList<>();

        Observable<Integer> source = Observable.from(1, 2, 3);

        Observable<Integer> mapped = new MapObservable<>(
                new MapObservable<>(source, x -> x * 2),
                x -> x + 1
        );

        mapped.subscribe(result::add);

        assertEquals(List.of(3, 5, 7), result);
    }

    @Test
    void map_shouldHandleEmptySource() {
        List<Integer> result = new ArrayList<>();

        Observable<Integer> source = Observable.create(observer -> {
            observer.onComplete();
        });

        Observable<Integer> mapped = new MapObservable<>(
                source,
                x -> x * 2
        );

        mapped.subscribe(result::add);

        assertTrue(result.isEmpty());
    }
}