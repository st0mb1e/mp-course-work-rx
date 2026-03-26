package ru.yartsev_vladislav.rx.internal;

import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.core.Observer;
import ru.yartsev_vladislav.rx.schedulers.Scheduler;

public class ObserveOnObservable<T> extends Observable<T> {
    public ObserveOnObservable(Observable<T> source, Scheduler scheduler) {
        super(observer -> {
            source.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    scheduler.execute(() -> observer.onNext(item));
                }

                @Override
                public void onError(Throwable t) {
                    scheduler.execute(() -> observer.onError(t));
                }

                @Override
                public void onComplete() {
                    scheduler.execute(observer::onComplete);
                }
            });
        });
    }
}
