package io.jdbd.mysql.env;

import io.jdbd.JdbdException;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.env.Key;

import java.util.Map;
import java.util.function.Supplier;

final class MySQLEnvironment implements Environment {


    static MySQLEnvironment from(Map<String, Object> properties) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <T> T get(Key<T> key) throws JdbdException {
        return null;
    }

    @Override
    public <T> T get(Key<T> key, Supplier<T> supplier) throws JdbdException {
        return null;
    }

    @Override
    public <T> T getOrDefault(Key<T> key) throws JdbdException {
        return null;
    }

    @Override
    public <T extends Comparable<T>> T getInRange(Key<T> key, T minValue, T maxValue) throws JdbdException {
        return null;
    }

    @Override
    public <T> T getRequired(Key<T> key) throws JdbdException {
        return null;
    }

    @Override
    public boolean isOn(Key<Boolean> key) throws JdbdException {
        return false;
    }

    @Override
    public boolean isOff(Key<Boolean> key) throws JdbdException {
        return false;
    }


}
