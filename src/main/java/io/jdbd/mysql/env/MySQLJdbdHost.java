package io.jdbd.mysql.env;

import io.jdbd.Driver;
import io.jdbd.JdbdException;
import io.jdbd.vendor.env.Environment;

import java.util.Map;

final class MySQLJdbdHost implements MySQLHost {


    static MySQLJdbdHost create(Protocol protocol, Map<String, Object> properties) {
        return new MySQLJdbdHost(protocol, properties);
    }


    private final Protocol protocol;

    private final String host;

    private final int port;

    private final String user;

    private final String password;

    private final String dbName;

    private final Environment env;

    private MySQLJdbdHost(final Protocol protocol, final Map<String, Object> properties) {
        this.protocol = protocol;
        this.host = (String) properties.remove(MySQLKey.HOST.name);
        this.port = (Integer) properties.remove(MySQLKey.PORT.name);
        this.user = (String) properties.remove(Driver.USER);

        this.password = (String) properties.remove(Driver.PASSWORD);
        this.dbName = (String) properties.remove(MySQLKey.DB_NAME.name);

        if (this.user == null) {
            throw new JdbdException("No user property");
        }

        assert this.host != null;

        this.env = MySQLEnvironment.from(properties);
    }


    @Override
    public Protocol protocol() {
        return this.protocol;
    }


    @Override
    public String host() {
        return this.host;
    }

    @Override
    public int port() {
        return this.port;
    }

    @Override
    public String user() {
        return this.user;
    }


    @Override
    public String password() {
        return this.password;
    }

    @Override
    public String dbName() {
        return this.dbName;
    }


    @Override
    public Environment properties() {
        return this.env;
    }


}
