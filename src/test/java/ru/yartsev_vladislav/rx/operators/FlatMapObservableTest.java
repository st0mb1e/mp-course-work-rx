package ru.yartsev_vladislav.rx.operators;

import org.junit.jupiter.api.Test;
import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.core.Observer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FlatMapObservableTest {

    @Test
    void shouldMapAndFlattenValues() {
        List<Integer> result = new ArrayList<>();

        Observable<Integer> source = Observable.from(1, 2, 3);

        new FlatMapObservable<>(source, x -> Observable.from(x, x * 10))
                .subscribe(new Observer<>() {
                    @Override
                    public void onNext(Integer item) {
                        result.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Should not error");
                    }

                    @Override
                    public void onComplete() {
                        // ok
                    }
                });

        assertEquals(List.of(1, 10, 2, 20, 3, 30), result);
    }

    @Test
    void shouldHandleEmptyInnerObservables() {
        List<Integer> result = new ArrayList<>();

        Observable<Integer> source = Observable.from(1, 2, 3);

        new FlatMapObservable<>(source, x -> Observable.<Integer>from())
                .subscribe(new Observer<>() {
                    @Override
                    public void onNext(Integer item) {
                        result.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail();
                    }

                    @Override
                    public void onComplete() {
                        // ok
                    }
                });

        assertTrue(result.isEmpty());
    }

    @Test
    void shouldCallOnErrorWhenMapperThrows() {
        Observable<Integer> source = Observable.from(1, 2, 3);

        List<String> events = new ArrayList<>();

        new FlatMapObservable<>(source, x -> {
            if (x == 2) throw new RuntimeException("boom");
            return Observable.from(x);
        }).subscribe(new Observer<>() {
            @Override
            public void onNext(Integer item) {
                events.add("next:" + item);
            }

            @Override
            public void onError(Throwable t) {
                events.add("error:" + t.getMessage());
            }

            @Override
            public void onComplete() {
                events.add("complete");
            }
        });

        assertTrue(events.contains("error:boom"));
        assertFalse(events.contains("complete"));
    }

    @Test
    void shouldCallOnErrorWhenInnerObservableErrors() {
        Observable<Integer> source = Observable.from(1, 2);

        List<String> events = new ArrayList<>();

        new FlatMapObservable<>(source, x -> {
            if (x == 2) {
                return Observable.create(observer -> observer.onError(new RuntimeException("fail")));
            }
            return Observable.from(x);
        }).subscribe(new Observer<>() {
            @Override
            public void onNext(Integer item) {
                events.add("next:" + item);
            }

            @Override
            public void onError(Throwable t) {
                events.add("error:" + t.getMessage());
            }

            @Override
            public void onComplete() {
                events.add("complete");
            }
        });

        assertTrue(events.contains("error:fail"));
        assertFalse(events.contains("complete"));
    }

    @Test
    void shouldNotEmitAfterError() {
        List<Integer> result = new ArrayList<>();

        Observable<Integer> source = Observable.from(1, 2, 3);

        new FlatMapObservable<>(source, x -> {
            if (x == 2) throw new RuntimeException("fail");
            return Observable.from(x);
        }).subscribe(new Observer<>() {
            @Override
            public void onNext(Integer item) {
                result.add(item);
            }

            @Override
            public void onError(Throwable t) {
                // ignore
            }

            @Override
            public void onComplete() {
                fail("Should not complete");
            }
        });

        assertEquals(List.of(1), result);
    }

    @Test
    void shouldCompleteAfterAllInnerObservables() {
        List<Integer> result = new ArrayList<>();
        boolean[] completed = { false };

        Observable<Integer> source = Observable.from(1, 2);

        new FlatMapObservable<>(source, x -> Observable.from(x, x * 10))
                .subscribe(new Observer<>() {
                    @Override
                    public void onNext(Integer item) {
                        result.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail();
                    }

                    @Override
                    public void onComplete() {
                        completed[0] = true;
                    }
                });

        assertTrue(completed[0]);
        assertEquals(List.of(1, 10, 2, 20), result);
    }
}