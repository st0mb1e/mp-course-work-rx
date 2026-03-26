package ru.yartsev_vladislav.rx.core;

import ru.yartsev_vladislav.rx.internal.ArrayOnSubscribe;
import ru.yartsev_vladislav.rx.internal.BooleanDisposable;
import ru.yartsev_vladislav.rx.internal.IterableOnSubscribe;
import ru.yartsev_vladislav.rx.internal.ObserveOnObservable;
import ru.yartsev_vladislav.rx.internal.SubscribeOnObservable;
import ru.yartsev_vladislav.rx.operators.FilterObservable;
import ru.yartsev_vladislav.rx.operators.FlatMapObservable;
import ru.yartsev_vladislav.rx.operators.MapObservable;
import ru.yartsev_vladislav.rx.schedulers.Scheduler;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {
    public interface OnSubscribe<T> {
        void subscribe(Observer<? super T> observer);
    }

    private final OnSubscribe<T> source;

    protected Observable(OnSubscribe<T> source) {
        this.source = source;
    }

    // Общий случай Observable
    public static <T> Observable<T> create(OnSubscribe<T> source) {
        return new Observable<>(source);
    }

    public static <T> Observable<T> create(Iterable<T> source) {
        return new Observable<>(new IterableOnSubscribe<>(source));
    }

    public static <T> Observable<T> from(T... items) {
        return create(new ArrayOnSubscribe<>(items));
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

    public Disposable subscribe(Consumer<? super T> onNext, Consumer<? super Throwable> onError) {
        return subscribe(onNext, onError, () -> {});
    }

    public Disposable subscribe(Consumer<? super T> onNext) {
        return subscribe(onNext, Throwable::printStackTrace);
    }

    public Observable<T> filter(Predicate<? super T> predicate) {
        return new FilterObservable<T>(this, predicate);
    }

    public <R> Observable<R> flatMap(Function<? super T, Observable<? extends R>> mapper) {
        return new FlatMapObservable<>(this, mapper);
    }

    public <R> Observable<R> map(Function<? super T, R> mapper) {
        return new MapObservable<T, R>(this, mapper);
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new SubscribeOnObservable<>(this, scheduler);
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return new ObserveOnObservable<>(this, scheduler);
    }
}
