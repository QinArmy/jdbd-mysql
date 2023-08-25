package io.jdbd.mysql.session;

import io.jdbd.DriverVersion;
import io.jdbd.meta.*;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.session.Option;
import org.reactivestreams.Publisher;

import java.util.function.Function;

final class MySQLDatabaseMetadata extends MySQLSessionMetaSpec implements DatabaseMetaData {

    static MySQLDatabaseMetadata create(MySQLProtocol protocol) {
        return new MySQLDatabaseMetadata(protocol);
    }

    private MySQLDatabaseMetadata(MySQLProtocol protocol) {
        super(protocol);
    }


    @Override
    public String productFamily() {
        return MySQLDriver.MY_SQL;
    }

    @Override
    public String productName() {
        return MySQLDriver.MY_SQL;
    }

    @Override
    public DriverVersion driverVersion() {
        return MySQLDriver.VERSION;
    }

    @Override
    public Publisher<DatabaseSchemaMetaData> currentSchema() {
        return null;
    }

    @Override
    public Publisher<DatabaseSchemaMetaData> schemas() {
        return null;
    }

    @Override
    public Publisher<DatabaseTableMetaData> tableOfCurrentSchema() {
        return null;
    }

    @Override
    public Publisher<DatabaseTableMetaData> tableOfSchema(DatabaseSchemaMetaData schemaMeta) {
        return null;
    }

    @Override
    public Publisher<DatabaseTableMetaData> tableOfSchema(DatabaseSchemaMetaData schemaMeta, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<TableColumnMetaData> columnOfTable(DatabaseTableMetaData tableMeta, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<TableIndexMetaData> indexOfTable(DatabaseTableMetaData tableMeta, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<DatabaseTableMetaData> tables(Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<TableColumnMetaData> columns(Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<TableIndexMetaData> indexes(Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public <R> Publisher<R> queryOption(Option<R> option) {
        return null;
    }

    @Override
    public Publisher<String> sqlKeyWords() {
        return null;
    }

    @Override
    public <T> T valueOf(Option<T> option) {
        return null;
    }


}
