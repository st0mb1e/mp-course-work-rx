package ru.yartsev_vladislav.rx.operators;

import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.core.Observer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class FlatMapObservable<T, R> extends Observable<R> {

    public FlatMapObservable(Observable<T> source, Function<? super T, Observable<? extends R>> mapper) {
        super(observer -> {
            AtomicInteger activeStreams = new AtomicInteger(1); // 1 для основного источника

            source.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    Observable<? extends R> mapped;
                    try {
                        mapped = mapper.apply(item);
                    } catch (Throwable t) {
                        observer.onError(t);
                        return;
                    }

                    activeStreams.incrementAndGet();
                    mapped.subscribe(new Observer<R>() {
                        @Override
                        public void onNext(R r) {
                            observer.onNext(r);
                        }

                        @Override
                        public void onError(Throwable t) {
                            observer.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            if (activeStreams.decrementAndGet() == 0) {
                                observer.onComplete();
                            }
                        }
                    });
                }

                @Override
                public void onError(Throwable t) {
                    observer.onError(t);
                }

                @Override
                public void onComplete() {
                    if (activeStreams.decrementAndGet() == 0) {
                        observer.onComplete();
                    }
                }
            });
        });
    }
}
