package ru.yartsev_vladislav;

import ru.yartsev_vladislav.rx.core.Observable;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        Observable<Integer> obs = Observable.create(Arrays.asList(1, 2, 3));
        obs.subscribe((item) -> {
            System.out.println("Sme-item " + item);
        });
    }
}
