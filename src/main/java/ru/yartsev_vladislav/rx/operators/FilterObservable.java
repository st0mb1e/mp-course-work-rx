package ru.yartsev_vladislav.rx.operators;

import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.core.Observer;

import java.util.function.Predicate;

public class FilterObservable<T> extends Observable<T> {
    public FilterObservable(Observable<T> source, Predicate<? super T> predicate) {
        super(observer -> {
            source.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    boolean passed;

                    try {
                        passed = predicate.test(item);
                    } catch (Throwable t) {
                        observer.onError(t);
                        return;
                    }

                    if (passed) {
                        observer.onNext(item);
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
