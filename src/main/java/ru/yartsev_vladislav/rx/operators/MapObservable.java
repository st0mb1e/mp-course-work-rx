package ru.yartsev_vladislav.rx.operators;

import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.core.Observer;

import java.util.function.Function;

public class MapObservable<E, T> extends Observable<T> {
    public MapObservable(Observable<E> source, Function<? super E, T> function) {
        super(observer -> {
            source.subscribe(new Observer<E>() {
                @Override
                public void onNext(E item) {
                    try {
                        observer.onNext(function.apply(item));
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
