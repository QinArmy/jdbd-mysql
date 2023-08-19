package io.jdbd.mysql.env;

import io.jdbd.JdbdException;
import io.jdbd.vendor.env.Environment;

import java.util.Map;
import java.util.function.Supplier;

final class MySQLEnvironment implements Environment {


    static MySQLEnvironment from(Map<String, Object> properties) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <T> T get(MySQLKey<T> key) throws JdbdException {
        return null;
    }

    @Override
    public <T> T get(MySQLKey<T> key, Supplier<T> supplier) throws JdbdException {
        return null;
    }

    @Override
    public <T> T getOrDefault(MySQLKey<T> key) throws JdbdException {
        return null;
    }

    @Override
    public <T extends Comparable<T>> T getInRange(MySQLKey<T> key, T minValue, T maxValue) throws JdbdException {
        return null;
    }

    @Override
    public <T> T getRequired(MySQLKey<T> key) throws JdbdException {
        return null;
    }


}
