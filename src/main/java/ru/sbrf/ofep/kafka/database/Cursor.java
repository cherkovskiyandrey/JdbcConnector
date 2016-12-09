package ru.sbrf.ofep.kafka.database;

import java.util.Objects;

public class Cursor {
    public static final Cursor UNDEFINED = Cursor.of(-1L);

    private final long offset;

    private Cursor(long offset) {
        this.offset = offset;
    }

    public static Cursor of(Long offset) {
        return new Cursor(offset);
    }

    public long getInnerOffset() {
        return offset;
    }

    public Cursor next() {
        return new Cursor(offset + 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cursor cursor = (Cursor) o;
        return offset == cursor.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset);
    }

    @Override
    public String toString() {
        return "Cursor{" +
                "offset=" + offset +
                '}';
    }
}
