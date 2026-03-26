package ru.yartsev_vladislav.rx.schedulers;

public interface Scheduler {
    void execute(Runnable task);
}
