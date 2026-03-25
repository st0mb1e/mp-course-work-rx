package ru.yartsev_vladislav.rx.internal;

import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.core.Observer;

public class ArrayOnSubscribe<T> implements Observable.OnSubscribe<T> {
    private final T[] array;

    public ArrayOnSubscribe(T[] array) {
        this.array = array;
    }

    @Override
    public void subscribe(Observer<? super T> observer) {
        for (int i = 0; i < array.length; i++) {
            observer.onNext(array[i]);
        }
        observer.onComplete();
    }
}
