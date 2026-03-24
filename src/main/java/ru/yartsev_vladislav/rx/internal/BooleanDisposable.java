package ru.yartsev_vladislav.rx.internal;

import ru.yartsev_vladislav.rx.core.Disposable;

import java.util.concurrent.atomic.AtomicBoolean;

public class BooleanDisposable implements Disposable {
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    @Override
    public void dispose() {
        disposed.set(true);
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
