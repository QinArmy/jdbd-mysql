package io.jdbd.postgre.config;

import io.jdbd.vendor.conf.AbstractHostInfo;
import io.jdbd.vendor.conf.ImmutableMapProperties;
import io.jdbd.vendor.conf.JdbcUrlParser;
import io.jdbd.vendor.conf.Properties;
import org.qinarmy.env.convert.ConverterManager;
import org.qinarmy.env.convert.ImmutableConverterManager;

import java.util.Map;

public final class PostgreHost extends AbstractHostInfo<PgKey> {

    public static PostgreHost create(JdbcUrlParser parser, int index) {
        return new PostgreHost(parser, index);
    }


    public static final int DEFAULT_PORT = 5432;


    private PostgreHost(JdbcUrlParser parser, int index) {
        super(parser, index);
    }

    @Override
    protected final PgKey getUserKey() {
        return PgKey.user;
    }

    @Override
    protected final PgKey getPasswordKey() {
        return PgKey.password;
    }

    @Override
    protected final PgKey getHostKey() {
        return PgKey.PGHOST;
    }

    @Override
    protected final PgKey getPortKey() {
        return PgKey.PGPORT;
    }

    @Override
    protected final PgKey getDbNameKey() {
        return PgKey.PGDBNAME;
    }

    @Override
    protected final int getDefaultPort() {
        return DEFAULT_PORT;
    }

    public final String getNonNullDbName() {
        String dbName = this.dbName;
        return dbName == null ? "" : dbName;
    }


    @Override
    protected final Properties<PgKey> createProperties(Map<String, String> map) {
        ConverterManager converterManager = ImmutableConverterManager.create(Converters::registerConverter);
        return ImmutableMapProperties.getInstance(map, converterManager);
    }


}