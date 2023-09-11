package io.jdbd.mysql.session;

import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.*;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.vendor.meta.VendorSchemaMeta;
import io.jdbd.vendor.meta.VendorTableMeta;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

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


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/show-databases.html"> SHOW DATABASES Statement</a>
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/extended-show.html">Extensions to SHOW Statements</a>
     */
    @Override
    public Publisher<SchemaMeta> schemas(final Function<Option<?>, ?> optionFunc) {
        final StringBuilder builder = new StringBuilder(30);
        builder.append("SHOW DATABASES");

        final Object nameValue;
        nameValue = optionFunc.apply(Option.NAME);

        if (nameValue instanceof String) {
            final boolean backslashEscapes;
            backslashEscapes = this.protocol.nonNullOf(Option.BACKSLASH_ESCAPES);

            if (((String) nameValue).indexOf(Constants.COMMA) < 0) {
                builder.append(" LIKE ");
                MySQLStrings.appendLiteral((String) nameValue, backslashEscapes, builder);
            } else {
                builder.append(" WHERE `Database` ");
                appendInPredicate((String) nameValue, backslashEscapes, builder, UnaryOperator.identity());
            }
        }
        return this.protocol.query(Stmts.stmt(builder.toString()), this::mapSchema, ResultStates.IGNORE_STATES);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-tables-table.html"> The INFORMATION_SCHEMA TABLES Table</a>
     */
    @Override
    public Publisher<TableMeta> tablesOfCurrentSchema(final Function<Option<?>, ?> optionFunc) {
        return queryTableMeta(null, optionFunc);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-tables-table.html"> The INFORMATION_SCHEMA TABLES Table</a>
     */
    @Override
    public Publisher<TableMeta> tablesOfSchema(final SchemaMeta schemaMeta, final Function<Option<?>, ?> optionFunc) {
        if (!(schemaMeta instanceof VendorSchemaMeta) || schemaMeta.isPseudoSchema()) {
            return Flux.error(MySQLExceptions.unknownSchemaMeta(schemaMeta));
        }
        return queryTableMeta(schemaMeta, optionFunc);
    }


    @Override
    public Publisher<TableColumnMeta> columnsOfTable(TableMeta tableMeta, Function<Option<?>, ?> optionFunc) {
        if (!(tableMeta instanceof VendorTableMeta)) {
            return Flux.error(MySQLExceptions.unknownTableMeta(tableMeta));
        }

        return null;
    }

    @Override
    public Publisher<TableIndexMeta> indexesOfTable(TableMeta tableMeta, Function<Option<?>, ?> optionFunc) {
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
     * @see #queryTableMeta(SchemaMeta, Function)
     */
    private SchemaMeta mapSchema(CurrentRow row) {
        return VendorSchemaMeta.fromSchema(this, "def", row.getNonNull(0, String.class), Option.EMPTY_OPTION_FUNC);
    }


    /**
     * @see #tablesOfCurrentSchema(Function)
     * @see #tablesOfSchema(SchemaMeta, Function)
     */
    private Flux<TableMeta> queryTableMeta(@Nullable SchemaMeta schemaMeta, Function<Option<?>, ?> optionFunc) {
        final String schemaName;
        if (schemaMeta == null) {
            schemaName = null;
        } else {
            schemaName = schemaMeta.schema();
        }
        final StringBuilder builder = new StringBuilder(256);
        builder.append("SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES")
                .append(" WHERE TABLE_SCHEMA ");

        final boolean backslashEscapes;
        backslashEscapes = this.protocol.nonNullOf(Option.BACKSLASH_ESCAPES);
        final UnaryOperator<String> jdbdToMySqlFunc = MySQLDatabaseMetadata::jdbdTableTypeToMySqlTableType;

        if (schemaName == null) {
            builder.append("= DATABASE()");
        } else {
            appendNamePredicate(schemaName, backslashEscapes, builder, jdbdToMySqlFunc);
        }

        final Object nameValue, typeValue;
        nameValue = optionFunc.apply(Option.NAME);
        typeValue = optionFunc.apply(Option.TYPE_NAME);

        if (nameValue instanceof String) {
            builder.append(" AND TABLE_NAME ");
            appendNamePredicate((String) nameValue, backslashEscapes, builder, jdbdToMySqlFunc);
        }
        if (typeValue instanceof String) {
            builder.append(" AND TABLE_TYPE ");
            appendNamePredicate((String) typeValue, backslashEscapes, builder, jdbdToMySqlFunc);
        }

        builder.append(" ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME");

        final SchemaMeta[] schemaMetaOfTableHolder = new SchemaMeta[1];

        final Function<CurrentRow, TableMeta> function;
        function = row -> {
            final SchemaMeta schemaMetaOfTable;
            if (schemaMeta != null) {
                schemaMetaOfTable = schemaMeta;
            } else if (schemaMetaOfTableHolder[0] == null) {
                schemaMetaOfTableHolder[0] = schemaMetaOfTable = mapSchema(row);
            } else {
                schemaMetaOfTable = schemaMetaOfTableHolder[0];
            }
            final String tableType;
            tableType = mysqlTableTypeToJdbdTableType(schemaMetaOfTable.schema(), row.getNonNull(2, String.class));
            final Function<Option<?>, ?> tableOptionFunc;
            tableOptionFunc = option -> option == Option.TYPE_NAME ? tableType : null;

            return VendorTableMeta.from(schemaMetaOfTable, row.getNonNull(1, String.class),
                    row.getNonNull(3, String.class), tableOptionFunc
            );
        };

        return this.protocol.query(Stmts.stmt(builder.toString()), function, ResultStates.IGNORE_STATES);
    }


    private void appendNamePredicate(final String name, final boolean backslashEscapes, final StringBuilder builder,
                                     final UnaryOperator<String> func) {
        if (name.indexOf(Constants.COMMA) > -1) {
            appendInPredicate(name, backslashEscapes, builder, func);
        } else if (name.indexOf('%') > -1) {
            builder.append("LIKE ");
            MySQLStrings.appendLiteral(name, backslashEscapes, builder);
        } else {
            builder.append("= ");
            MySQLStrings.appendLiteral(func.apply(name), backslashEscapes, builder);
        }

    }

    /**
     * This method don't append space before predicate.
     *
     * @see #tablesOfSchema(SchemaMeta, Function)
     * @see #schemas(Function)
     */
    private void appendInPredicate(final String nameSet, boolean backslashEscapes, final StringBuilder builder
            , final UnaryOperator<String> func) {
        final String[] nameArray = nameSet.split(",");
        builder.append("IN (");

        final Map<String, Boolean> map = MySQLCollections.hashMap((int) (nameArray.length / 0.75f));
        String value;
        for (int i = 0, outCount = 0; i < nameArray.length; i++) {
            value = func.apply(nameArray[i]);
            if (value != null && map.putIfAbsent(value, Boolean.TRUE) != null) {
                continue;
            }
            if (outCount > 0) {
                builder.append(Constants.COMMA);
            }
            MySQLStrings.appendLiteral(value, backslashEscapes, builder);
            outCount++;
        }
        builder.append(')');

    }

    /*-------------------below private static methods -------------------*/


    /**
     * @see #queryTableMeta(SchemaMeta, Function)
     */
    private static String mysqlTableTypeToJdbdTableType(final String schemaName, final String mysqlTableType) {
        final String jdbdTableType;
        switch (mysqlTableType.toUpperCase(Locale.ROOT)) {
            case "BASE TABLE": {
                switch (schemaName.toLowerCase(Locale.ROOT)) {
                    case "mysql":
                    case "performance_schema":
                        jdbdTableType = TableMeta.SYSTEM_TABLE;
                        break;
                    default:
                        jdbdTableType = TableMeta.TABLE;
                }
            }
            break;
            case "TEMPORARY":
                jdbdTableType = "LOCAL_TEMPORARY";
                break;
            default:
                jdbdTableType = mysqlTableType;

        }
        return jdbdTableType;
    }

    /**
     * @see #queryTableMeta(SchemaMeta, Function)
     */
    private static String jdbdTableTypeToMySqlTableType(final String jdbdTableType) {
        final String tableType;
        switch (jdbdTableType) {
            case TableMeta.TABLE:
            case TableMeta.SYSTEM_TABLE:
                tableType = "BASE TABLE";
                break;
            case TableMeta.LOCAL_TEMPORARY:
                tableType = "TEMPORARY";
                break;
            default:
                tableType = jdbdTableType;
        }
        return tableType;
    }


}
