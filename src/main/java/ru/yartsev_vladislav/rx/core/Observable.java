package ru.yartsev_vladislav.rx.core;

import ru.yartsev_vladislav.rx.internal.BooleanDisposable;
import ru.yartsev_vladislav.rx.internal.IterableOnSubscribe;

import java.util.function.Consumer;

public class Observable<T> {

    public interface OnSubscribe<T> {
        void subscribe(Observer<? super T> observer);
    }

    private final OnSubscribe<T> source;

    private Observable(OnSubscribe<T> source) {
        this.source = source;
    }

    // Общий случай Observable
    public static <T> Observable<T> create(OnSubscribe<T> source) {
        return new Observable<>(source);
    }

    public static <T> Observable<T> create(Iterable<T> source) {
        return new Observable<>(new IterableOnSubscribe<>(source));
    }

    // базовый subscribe
    public Disposable subscribe(Observer<? super T> observer) {
        BooleanDisposable disposable = new BooleanDisposable();

        try {
            source.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    if (!disposable.isDisposed()) {
                        observer.onNext(item);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    if (!disposable.isDisposed()) {
                        observer.onError(t);
                        disposable.dispose();
                    }
                }

                @Override
                public void onComplete() {
                    if (!disposable.isDisposed()) {
                        observer.onComplete();
                        disposable.dispose();
                    }
                }
            });
        } catch (Throwable t) {
            observer.onError(t);
        }

        return disposable;
    }

    public Disposable subscribe(
            Consumer<? super T> onNext,
            Consumer<? super Throwable> onError,
            Runnable onComplete
    ) {
        return subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                onNext.accept(item);
            }

            @Override
            public void onError(Throwable t) {
                onError.accept(t);
            }

            @Override
            public void onComplete() {
                onComplete.run();
            }
        });
    }

    public Disposable subscribe(Consumer<? super T> onNext) {
        return subscribe(onNext, Throwable::printStackTrace, () -> {});
    }
}
