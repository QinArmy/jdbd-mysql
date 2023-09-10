package io.jdbd.mysql.session;

import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.*;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.vendor.meta.VendorSchemaMeta;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;

import java.util.function.Function;

final class MySQLDatabaseMetadata extends MySQLSessionMetaSpec implements DatabaseMetaData {

    static MySQLDatabaseMetadata create(MySQLDatabaseSession<?> session) {
        return new MySQLDatabaseMetadata(session);
    }


    private final MySQLDatabaseSession<?> session;

    private MySQLDatabaseMetadata(MySQLDatabaseSession<?> session) {
        super(session.protocol);
        this.session = session;
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
        return MySQLDriver.getInstance().version();
    }


    @Override
    public Publisher<SchemaMeta> currentSchema() {
        return this.protocol.query(Stmts.stmt("SELECT DATABASE() AS cs"), this::mapSchema, ResultStates.IGNORE_STATES)
                .last();
    }

    @Override
    public Publisher<SchemaMeta> schemas(final Function<Option<?>, ?> optionFunc) {
        final StringBuilder builder = new StringBuilder(30);
        builder.append("SHOW DATABASES");

        final Object nameValue;
        nameValue = optionFunc.apply(Option.NAME);
        if (nameValue instanceof String) {
            builder.append(" LIKE ");
            MySQLStrings.appendLiteral((String) nameValue, this.protocol.nonNullOf(Option.BACKSLASH_ESCAPES), builder);
        }
        return this.protocol.query(Stmts.stmt(builder.toString()), this::mapSchema, ResultStates.IGNORE_STATES);
    }

    @Override
    public Publisher<TableMeta> tableOfCurrentSchema(final Function<Option<?>, ?> optionFunc) {
        final StringBuilder builder = new StringBuilder();
        builder.append("SHOW FULL TABLES FROM DATABASE()");
        return null;
    }

    @Override
    public Publisher<TableMeta> tableOfSchema(SchemaMeta schemaMeta, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<TableColumnMetaData> columnOfTable(TableMeta tableMeta, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<TableIndexMetaData> indexOfTable(TableMeta tableMeta, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<TableMeta> tables(Function<Option<?>, ?> optionFunc) {
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
    public String identifierQuoteString() {
        return null;
    }

    @Override
    public Publisher<FunctionMeta> sqlFunctions(@Nullable SchemaMeta metaData, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<FunctionColumnMeta> sqlFunctionColumn(@Nullable SchemaMeta metaData, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<FunctionColumnMeta> sqlFunctionColumnOf(FunctionMeta functionMeta, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<ProcedureMeta> sqlProcedures(@Nullable SchemaMeta metaData, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<ProcedureColumnMeta> sqlProcedureColumn(@Nullable SchemaMeta metaData, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public Publisher<ProcedureColumnMeta> sqlProcedureColumnOf(FunctionMeta functionMeta, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public int sqlStateType() throws JdbdException {
        return 0;
    }

    @Override
    public Publisher<DataTypeMeta> sqlDataTypes() {
        return null;
    }

    @Override
    public <T> T valueOf(Option<T> option) {
        return null;
    }

    @Override
    public DatabaseSession getSession() {
        return this.session;
    }

    @Override
    public <T extends DatabaseSession> T getSession(Class<T> sessionClass) {
        return sessionClass.cast(this.session);
    }


    /*-------------------below private instance method -------------------*/

    /**
     * @see #currentSchema()
     */
    private SchemaMeta mapSchema(CurrentRow row) {
        return VendorSchemaMeta.from(this, null, row.getNonNull(0, String.class));
    }


}
