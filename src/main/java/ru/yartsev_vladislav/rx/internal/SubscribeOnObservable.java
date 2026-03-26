package ru.yartsev_vladislav.rx.internal;

import ru.yartsev_vladislav.rx.core.Observable;
import ru.yartsev_vladislav.rx.schedulers.Scheduler;

public class SubscribeOnObservable<T> extends Observable<T> {
    public SubscribeOnObservable(Observable<T> source, Scheduler scheduler) {
        super(observer -> {
            scheduler.execute(() -> source.subscribe(observer));
        });
    }
}
