package io.jdbd.mysql.session;

import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.*;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.client.Charsets;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.vendor.meta.VendorSchemaMeta;
import io.jdbd.vendor.meta.VendorTableColumnMeta;
import io.jdbd.vendor.meta.VendorTableMeta;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.jdbd.vendor.meta.VendorTableColumnMeta.*;

final class MySQLDatabaseMetadata extends MySQLSessionMetaSpec implements DatabaseMetaData {

    static MySQLDatabaseMetadata create(MySQLDatabaseSession<?> session) {
        return new MySQLDatabaseMetadata(session);
    }


    private static final Logger LOG = LoggerFactory.getLogger(MySQLDatabaseMetadata.class);

    private static final Option<String> ENGINE = Option.from("ENGINE", String.class);


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
        if (!(schemaMeta instanceof VendorSchemaMeta)
                || schemaMeta.databaseMetadata() != this
                || schemaMeta.isPseudoSchema()) {
            return Flux.error(MySQLExceptions.unknownSchemaMeta(schemaMeta));
        }
        return queryTableMeta(schemaMeta, optionFunc);
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-columns-table.html">The INFORMATION_SCHEMA COLUMNS Table</a>
     */
    @Override
    public Publisher<TableColumnMeta> columnsOfTable(final TableMeta tableMeta, final Function<Option<?>, ?> optionFunc) {
        final SchemaMeta schemaMeta;
        if (!(tableMeta instanceof VendorTableMeta)
                || (schemaMeta = tableMeta.schemaMeta()).isPseudoSchema()
                || schemaMeta.databaseMetadata() != this) {
            return Flux.error(MySQLExceptions.unknownTableMeta(tableMeta));
        }

        final StringBuilder builder = new StringBuilder(460);
        builder.append("SELECT COLUMN_NAME,DATA_TYPE,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,ORDINAL_POSITION")
                .append(",DATETIME_PRECISION,NUMERIC_PRECISION,NUMERIC_SCALE,EXTRA,COLUMN_COMMENT,CHARACTER_SET_NAME")
                .append(",CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,COLLATION_NAME,PRIVILEGES")
                .append(" FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ");

        final boolean backslashEscapes;
        backslashEscapes = this.protocol.nonNullOf(Option.BACKSLASH_ESCAPES);

        MySQLStrings.appendLiteral(schemaMeta.schema(), backslashEscapes, builder);

        builder.append(" AND TABLE_NAME = ");

        MySQLStrings.appendLiteral(tableMeta.tableName(), backslashEscapes, builder);


        final Object nameValue;
        nameValue = optionFunc.apply(Option.NAME);
        if (nameValue instanceof String) {
            builder.append(" AND COLUMN_NAME ");
            appendNamePredicate((String) nameValue, backslashEscapes, builder, UnaryOperator.identity());
        }

        builder.append(" ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION");


        final Function<CurrentRow, TableColumnMeta> function;
        function = row -> {

            final MySQLType dataType;
            dataType = mapColumnDataType(row);

            final Map<Option<?>, Object> map = MySQLCollections.hashMap((int) (13 / 0.75f));

            map.put(Option.NAME, row.getNonNull("COLUMN_NAME", String.class));
            map.put(COLUMN_DATA_TYPE, dataType);
            map.put(COLUMN_POSITION, row.getNonNull("ORDINAL_POSITION", Integer.class));
            map.put(Option.PRECISION, mapColumnPrecision(dataType, row));

            map.put(COLUMN_SCALE, mapColumnScale(dataType, row));
            map.put(COLUMN_NULLABLE_MODE, row.getOrDefault("IS_NULLABLE", BooleanMode.class, BooleanMode.UNKNOWN));
            map.put(COLUMN_AUTO_INCREMENT_MODE, mapAutoIncrementMode(row));
            map.put(COLUMN_GENERATED_MODE, mapGeneratedMode(row));

            map.put(COLUMN_DEFAULT, row.getString("COLUMN_DEFAULT"));
            map.put(COLUMN_COMMENT, row.getString("COLUMN_COMMENT"));
            map.put(Option.CHARSET, Charsets.getJavaCharsetByCharsetName(row.getString("CHARACTER_SET_NAME")));
            map.put(Option.COLLATION, row.getString("COLLATION_NAME"));

            map.put(Option.PRIVILEGE, row.getString("PRIVILEGES"));

            final Function<Class<?>, Set<?>> enumSetFunc;
            enumSetFunc = createEnumSetFunc(dataType, row);

            return VendorTableColumnMeta.from(tableMeta, enumSetFunc, map::get);
        };
        return this.protocol.query(Stmts.stmt(builder.toString()), function, ResultStates.IGNORE_STATES);
    }


    @Override
    public Publisher<TableIndexMeta> indexesOfTable(final TableMeta tableMeta, final Function<Option<?>, ?> optionFunc) {
        final SchemaMeta schemaMeta;
        if (!(tableMeta instanceof VendorTableMeta)
                || (schemaMeta = tableMeta.schemaMeta()).isPseudoSchema()
                || schemaMeta.databaseMetadata() != this) {
            return Flux.error(MySQLExceptions.unknownTableMeta(tableMeta));
        }

        return Flux.empty();
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
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-tables-table.html"> The INFORMATION_SCHEMA TABLES Table</a>
     */
    private Flux<TableMeta> queryTableMeta(@Nullable SchemaMeta schemaMeta, Function<Option<?>, ?> optionFunc) {
        final String schemaName;
        if (schemaMeta == null) {
            schemaName = null;
        } else {
            schemaName = schemaMeta.schema();
        }
        final StringBuilder builder = new StringBuilder(256);
        builder.append("SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,TABLE_COMMENT,ENGINE,TABLE_COLLATION")
                .append(" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA ");

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


            final String tableType, collation;
            tableType = mysqlTableTypeToJdbdTableType(schemaMetaOfTable.schema(), row.getNonNull(2, String.class));
            collation = row.get(5, String.class);

            final Map<Option<?>, Object> optionMap = MySQLCollections.hashMap(7);

            optionMap.put(Option.TYPE_NAME, tableType);
            optionMap.put(ENGINE, row.get(4, String.class));
            optionMap.put(Option.COLLATION, collation);
            optionMap.put(Option.CHARSET, Charsets.getJavaCharsetByCollationName(collation));


            return VendorTableMeta.from(schemaMetaOfTable, row.getNonNull(1, String.class),
                    row.getNonNull(3, String.class), optionMap::get
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

    /**
     * @see #columnsOfTable(TableMeta, Function)
     */
    private long mapColumnPrecision(final MySQLType dataType, final CurrentRow row) {
        final long precision;
        switch (dataType) {
            case BIT:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                precision = row.getNonNull("NUMERIC_PRECISION", Long.class);
                break;
            case CHAR:
            case VARCHAR:

            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                precision = row.getNonNull("CHARACTER_MAXIMUM_LENGTH", Long.class);
                break;

            case BINARY:
            case VARBINARY:

            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                precision = row.getNonNull("CHARACTER_OCTET_LENGTH", Long.class);
                break;
            default:
                precision = 0;
        }
        return precision;
    }

    /**
     * @see #columnsOfTable(TableMeta, Function)
     */
    private int mapColumnScale(final MySQLType dataType, final CurrentRow row) {
        final int scale;
        switch (dataType) {
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                scale = row.getNonNull("NUMERIC_SCALE", Integer.class);
                break;
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                scale = row.getNonNull("DATETIME_PRECISION", Integer.class);
                break;
            default:
                scale = 0;
        }
        return scale;
    }

    /**
     * @see #columnsOfTable(TableMeta, Function)
     */
    @SuppressWarnings("deprecation")
    private MySQLType mapColumnDataType(final CurrentRow row) {
        final String dataType, columnType;
        dataType = row.getNonNullString("DATA_TYPE").toUpperCase(Locale.ROOT);
        columnType = row.getNonNullString("COLUMN_TYPE").toUpperCase(Locale.ROOT);

        final MySQLType type, finalType;
        type = MySQLBinds.MYSQL_TYPE_MAP.getOrDefault(dataType, MySQLType.UNKNOWN);

        if (!columnType.contains("UNSIGNED")) {
            finalType = type;
        } else switch (type) {
            case TINYINT:
                finalType = MySQLType.TINYINT_UNSIGNED;
                break;
            case SMALLINT:
                finalType = MySQLType.SMALLINT_UNSIGNED;
                break;
            case MEDIUMINT:
                finalType = MySQLType.MEDIUMINT_UNSIGNED;
                break;
            case INT:
                finalType = MySQLType.INT_UNSIGNED;
                break;
            case BIGINT:
                finalType = MySQLType.BIGINT_UNSIGNED;
                break;
            case DOUBLE:
                finalType = MySQLType.DOUBLE_UNSIGNED;
                break;
            case FLOAT:
                finalType = MySQLType.FLOAT_UNSIGNED;
                break;
            case DECIMAL:
                finalType = MySQLType.DECIMAL_UNSIGNED;
                break;
            default:
                finalType = type;
        }

        return finalType;
    }

    /**
     * @see #columnsOfTable(TableMeta, Function)
     */
    private BooleanMode mapAutoIncrementMode(CurrentRow row) {
        final BooleanMode mode;
        if (row.getNonNullString("EXTRA").equalsIgnoreCase("auto_increment")) {
            mode = BooleanMode.TRUE;
        } else {
            mode = BooleanMode.FALSE;
        }
        return mode;
    }

    /**
     * @see #columnsOfTable(TableMeta, Function)
     */
    private BooleanMode mapGeneratedMode(CurrentRow row) {
        final BooleanMode mode;
        if (row.getNonNullString("EXTRA").toUpperCase(Locale.ROOT).contains("GENERATED")) {
            mode = BooleanMode.TRUE;
        } else {
            mode = BooleanMode.FALSE;
        }
        return mode;
    }


    /**
     * @return a unmodified set
     * @see #createEnumSetFunc(MySQLType, CurrentRow)
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-columns-table.html">The INFORMATION_SCHEMA COLUMNS Table</a>
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/string-literals.html#character-escape-sequences">Special Character Escape Sequences</a>
     */
    private Set<String> mapEnumElementSet(final CurrentRow row) {
        // eg 1 : enum('T','F')
        // eg 2 : set('BEIJING','SHANGHAI','SHENZHEN','XIANGGANG','TAIBEI','AOMENG')
        final String definition;
        definition = row.getString("COLUMN_TYPE");
        if (definition == null) {
            return Collections.emptySet();
        }
        final int leftIndex, rightIndex;
        leftIndex = definition.indexOf('(');
        rightIndex = definition.lastIndexOf(')');

        if (leftIndex < 0 || rightIndex < 0) {
            return Collections.emptySet();
        }

        try {
            final boolean backslashEscapes;
            backslashEscapes = this.protocol.nonNullOf(Option.BACKSLASH_ESCAPES);

            final int length = definition.length(), lasIndex = length - 1;

            final Set<String> set = MySQLCollections.hashSet();
            final StringBuilder builder = new StringBuilder(15);

            int commaCount = 0;
            boolean inQuote = false;
            char ch, nextChar;
            for (int i = 0, lastWritten = -1; i < length; i++) {
                ch = definition.charAt(i);
                if (inQuote) {
                    if (backslashEscapes && ch == Constants.BACK_SLASH) {
                        if (i == lasIndex) {
                            i++;
                            continue;
                        }
                        switch (nextChar = definition.charAt(i + 1)) {
                            case '0':
                                nextChar = Constants.NUL;
                                break;
                            case 'b':
                                nextChar = '\b';
                                break;
                            case 'n':
                                nextChar = '\n';
                                break;
                            case 'r':
                                nextChar = '\r';
                                break;
                            case 't':
                                nextChar = '\t';
                                break;
                            case 'Z':
                                nextChar = '\032';
                                break;
                            default:
                                // no-op
                        }
                        if (i > lastWritten) {
                            builder.append(definition, lastWritten, i);
                        }
                        builder.append(nextChar);

                        i++;

                        lastWritten = i + 1; // after i ++

                    } else if (ch != Constants.QUOTE) {
                        continue;
                    } else if (i < lasIndex && definition.charAt(i + 1) == Constants.QUOTE) {
                        if (i > lastWritten) {
                            builder.append(definition, lastWritten, i); // append before i ++;
                        }
                        i++;
                        lastWritten = i; // after i ++,not i + 1, quote not append
                    } else {
                        inQuote = false;
                        if (i > lastWritten) {
                            builder.append(definition, lastWritten, i);
                        }
                        set.add(builder.toString());
                        builder.setLength(0); // clear
                        lastWritten = -1;
                    }

                    continue;

                } //  if (inQuote)

                if (ch == Constants.QUOTE) {
                    inQuote = true;
                    lastWritten = i + 1;
                } else if (ch == Constants.COMMA) {
                    commaCount++;
                }
            }
            if (inQuote || commaCount + 1 != set.size()) {
                throw new JdbdException(String.format("%s isn't enum or set type definition", definition));
            }
            return Collections.unmodifiableSet(set);
        } catch (JdbdException e) {
            throw e;
        } catch (IndexOutOfBoundsException e) {
            throw new JdbdException(String.format("%s isn't enum or set type definition", definition), e);
        } catch (Exception e) {
            throw MySQLExceptions.wrap(e);
        }
    }

    /**
     * @see #columnsOfTable(TableMeta, Function)
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Function<Class<?>, Set<?>> createEnumSetFunc(final MySQLType dataType, final CurrentRow row) {
        final Function<Class<?>, Set<?>> enumSetFunc;
        switch (dataType) {
            case ENUM:
            case SET: {
                final Set<String> enumSet;
                enumSet = mapEnumElementSet(row);
                enumSetFunc = clazz -> {
                    final Set<?> set;
                    if (clazz == String.class) {
                        set = enumSet;
                    } else if (!Enum.class.isAssignableFrom(clazz)) {
                        set = Collections.emptySet();
                    } else if (clazz.isAnonymousClass()) {
                        set = MySQLStrings.textSetToEnumSet(enumSet, (Class<? extends Enum>) clazz.getSuperclass(), true);
                    } else {
                        set = MySQLStrings.textSetToEnumSet(enumSet, (Class<? extends Enum>) clazz, true);
                    }
                    return set;
                };
            }
            break;
            default:
                enumSetFunc = EMPTY_ENUMS_FUNC;
        }
        return enumSetFunc;
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
