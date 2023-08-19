package io.jdbd.mysql;

import io.jdbd.Driver;
import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.mysql.env.MySQLUrlParser;
import io.jdbd.mysql.session.MySQLDatabaseSessionFactory;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.util.DefaultDriverVersion;

import java.util.Map;
import java.util.Objects;

public final class MySQLDriver implements Driver {


    public static Driver getInstance() {
        return INSTANCE;
    }

    public static final String MY_SQL = "MySQL";

    public static final String DRIVER_VENDOR = "io.jdbd.mysql";

    private static final MySQLDriver INSTANCE = new MySQLDriver();

    private final DriverVersion version;

    private MySQLDriver() {
        this.version = DefaultDriverVersion.from(MySQLDriver.class.getName(), MySQLDriver.class);
    }

    @Override
    public boolean acceptsUrl(final String url) {
        return MySQLUrlParser.acceptsUrl(url);
    }

    @Override
    public DatabaseSessionFactory forDeveloper(String url, Map<String, Object> properties)
            throws JdbdException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(properties, "properties");
        return MySQLDatabaseSessionFactory.create(url, properties);
    }

    @Override
    public DatabaseSessionFactory forPoolVendor(String url, Map<String, Object> properties)
            throws JdbdException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(properties, "properties");
        return MySQLDatabaseSessionFactory.forPoolVendor(url, properties);
    }


    @Override
    public String productName() {
        return MY_SQL;
    }

    @Override
    public String vendor() {
        return DRIVER_VENDOR;
    }

    @Override
    public DriverVersion version() {
        return this.version;
    }


    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getName())
                .append("[ vendor : ")
                .append(vendor())
                .append(" , productName : ")
                .append(productName())
                .append(" , version : ")
                .append(version())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
