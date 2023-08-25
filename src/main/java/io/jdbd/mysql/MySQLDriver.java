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

/**
 * <p>
 * This class is a implementation of {@link Driver} with MySQL.
 * </p>
 *
 * @since 1.0
 */
public final class MySQLDriver implements Driver {


    public static Driver getInstance() {
        return INSTANCE;
    }

    public static final String MY_SQL = "MySQL";

    public static final String DRIVER_VENDOR = "io.jdbd.mysql";

    private static final DriverVersion VERSION = DefaultDriverVersion.from(MySQLDriver.class.getName(), MySQLDriver.class);

    private static final MySQLDriver INSTANCE = new MySQLDriver();

    /**
     * private constructor
     */
    private MySQLDriver() {
    }

    @Override
    public boolean acceptsUrl(final String url) {
        return MySQLUrlParser.acceptsUrl(url);
    }

    @Override
    public DatabaseSessionFactory forDeveloper(String url, Map<String, Object> properties)
            throws JdbdException {
        return MySQLDatabaseSessionFactory.create(url, properties, false);
    }

    @Override
    public DatabaseSessionFactory forPoolVendor(String url, Map<String, Object> properties)
            throws JdbdException {
        return MySQLDatabaseSessionFactory.create(url, properties, true);
    }


    @Override
    public String productFamily() {
        return MY_SQL;
    }

    @Override
    public String vendor() {
        return DRIVER_VENDOR;
    }

    @Override
    public DriverVersion version() {
        return VERSION;
    }


    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getName())
                .append("[ vendor : ")
                .append(vendor())
                .append(" , productFamily : ")
                .append(productFamily())
                .append(" , version : ")
                .append(version())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
