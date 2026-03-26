package ru.yartsev_vladislav.rx.schedulers;

public class Schedulers {
    public static Scheduler io() {
        return new IOThreadScheduler();
    }

    public static Scheduler computation() {
        return new ComputationScheduler();
    }

    public static Scheduler single() {
        return new SingleThreadScheduler();
    }
}
