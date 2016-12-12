package ru.sbrf.ofep.kafka.database.simpleloader;

public interface Function<R, K, E extends Throwable> {
    R execute(K result) throws E;
}


