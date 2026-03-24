package ru.yartsev_vladislav.rx.internal;

import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.core.Observer;

public class IterableOnSubscribe<T> implements Observable.OnSubscribe<T> {
    private final Iterable<T> iterable;

    public IterableOnSubscribe(Iterable<T> iterable) {
        this.iterable = iterable;
    }

    @Override
    public void subscribe(Observer<? super T> observer) {
        for (T item : iterable) {
            observer.onNext(item);
        }
        observer.onComplete();
    }
}
