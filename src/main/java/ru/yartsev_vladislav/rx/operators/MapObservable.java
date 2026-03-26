package ru.yartsev_vladislav.rx.operators;

import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.core.Observer;

import java.util.function.Function;

public class MapObservable<R, T> extends Observable<T> {
    public MapObservable(Observable<R> source, Function<? super R, T> mapper) {
        super(observer -> {
            source.subscribe(new Observer<R>() {
                @Override
                public void onNext(R item) {
                    try {
                        observer.onNext(mapper.apply(item));
                    } catch (Throwable t) {
                        observer.onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    observer.onError(t);
                }

                @Override
                public void onComplete() {
                    observer.onComplete();
                }
            });
        });
    }
}
