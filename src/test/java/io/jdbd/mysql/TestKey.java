package io.jdbd.mysql;

import io.jdbd.lang.Nullable;
import io.jdbd.vendor.env.Key;

public final class TestKey<T> extends Key<T> {


    public static final TestKey<Boolean> TRUNCATE_AFTER_SUITE = new TestKey<>("truncate.after.suite", Boolean.class, Boolean.TRUE);

    private TestKey(String name, Class<T> valueClass, @Nullable T defaultValue) {
        super(name, valueClass, defaultValue);
    }


}
