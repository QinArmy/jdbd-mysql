package io.jdbd.mysql.env;

import io.jdbd.lang.Nullable;

import java.util.Map;

public interface MySQLEnvironment {

    @Nullable
    <T> T get(MySQLKey<T> key);

    <T> T getOrDefault(MySQLKey<T> key);

    <T> T getRequired(MySQLKey<T> key);


    static MySQLEnvironment parse(String url, Map<String, Object> properties) {
        throw new UnsupportedOperationException();
    }

}