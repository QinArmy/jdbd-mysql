package io.jdbd.mysql.env;

import io.jdbd.Driver;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.env.SimpleEnvironment;

import java.util.Map;

final class MySQLJdbdHost implements MySQLHostInfo {


    static MySQLJdbdHost create(Protocol protocol, Map<String, Object> properties) {
        final MySQLJdbdHost host;
        host = new MySQLJdbdHost(protocol, properties);
        // check all key value
        final Environment env = host.env;
        for (MySQLKey<?> key : MySQLKey.keyList()) {
            env.get(key);
        }
        return host;
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
        this.password = (String) properties.remove(Driver.PASSWORD);

        final Environment env = SimpleEnvironment.from(properties);
        this.env = env;

        this.host = env.getOrDefault(MySQLKey.HOST);
        this.port = env.getOrDefault(MySQLKey.PORT);
        this.user = env.getOrDefault(MySQLKey.USER);
        this.dbName = env.getOrDefault(MySQLKey.DB_NAME);

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

    @Override
    public boolean isUnixDomainSocket() {
        return this.host.indexOf('/') > -1;
    }


}
