package ru.sbrf.ofep.kafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Utils {
    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception ioe) {
            // ignore
        }
    }

    public static <T> Set<T> asImmutableSet(T... args) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(args)));
    }

    public static String toDBFormat(String field) {
        return field.toUpperCase();
    }
}
