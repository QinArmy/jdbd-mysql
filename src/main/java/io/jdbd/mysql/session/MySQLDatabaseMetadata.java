/*
 * Copyright 2023-2043 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.util.SqlLogger;
import io.jdbd.vendor.VendorOptions;
import io.jdbd.vendor.meta.*;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;


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
    public Publisher<SchemaMeta> currentSchema(final Function<Option<?>, ?> optionFunc) {
        final String sql = "SELECT DATABASE() AS cs";
        SqlLogger.printLog(optionFunc, sql);
        return this.protocol.query(Stmts.stmt(sql), this::mapSchema, ResultStates.IGNORE_STATES)
                .last();
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/show-databases.html"> SHOW DATABASES Statement</a>
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/extended-show.html">Extensions to SHOW Statements</a>
     */
    @Override
    public Publisher<SchemaMeta> schemas(final Function<Option<?>, ?> optionFunc) {
        final Object nameValue, catalogValue;
        catalogValue = optionFunc.apply(DatabaseMetaData.CATALOG);
        if (catalogValue instanceof String && !catalogValue.equals("def")) {
            return Flux.empty();
        }

        final StringBuilder builder = new StringBuilder(30);
        builder.append("SHOW DATABASES");

        nameValue = optionFunc.apply(Option.NAME);

        if (nameValue instanceof String) {
            final boolean backslashEscapes;
            backslashEscapes = this.protocol.nonNullOf(Option.BACKSLASH_ESCAPES);
            appendNamePredicate((String) nameValue, backslashEscapes, builder, UnaryOperator.identity());
        }
        final String sql = builder.toString();
        SqlLogger.printLog(optionFunc, sql);
        return this.protocol.query(Stmts.stmt(sql), this::mapSchema, ResultStates.IGNORE_STATES);
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
            map.put(VendorOptions.DATA_TYPE, dataType);
            map.put(VendorOptions.POSITION, row.getNonNull("ORDINAL_POSITION", Integer.class));
            map.put(Option.PRECISION, mapColumnPrecision(dataType, row));

            map.put(VendorOptions.SCALE, mapColumnScale(dataType, row));
            map.put(VendorOptions.NULLABLE_MODE, row.getOrDefault("IS_NULLABLE", BooleanMode.class, BooleanMode.UNKNOWN));
            map.put(VendorOptions.AUTO_INCREMENT_MODE, mapAutoIncrementMode(row));
            map.put(VendorOptions.GENERATED_MODE, mapGeneratedMode(row));

            map.put(VendorOptions.DEFAULT_VALUE, row.get("COLUMN_DEFAULT", String.class));
            map.put(VendorOptions.COMMENT, row.get("COLUMN_COMMENT", String.class));
            map.put(Option.CHARSET, Charsets.getJavaCharsetByCharsetName(row.get("CHARACTER_SET_NAME", String.class)));
            map.put(Option.COLLATION, row.get("COLLATION_NAME", String.class));

            map.put(Option.PRIVILEGE, row.get("PRIVILEGES", String.class));

            final Function<Class<?>, Set<?>> enumSetFunc;
            enumSetFunc = createEnumSetFunc(dataType, row);

            return VendorTableColumnMeta.from(tableMeta, enumSetFunc, map);
        };

        final String sql = builder.toString();
        SqlLogger.printLog(optionFunc, sql);
        return this.protocol.query(Stmts.stmt(sql), function, ResultStates.IGNORE_STATES);
    }

    /**
     * <p>
     * optionFunc support dialect following dialect options :
     *     <ul>
     *         <li>{@link VendorOptions#INDEX_TYPE}</li>
     *     </ul>
     * <br/>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-statistics-table.html"> The INFORMATION_SCHEMA STATISTICS Table</a>
     */
    @Override
    public Publisher<TableIndexMeta> indexesOfTable(final TableMeta tableMeta, final Function<Option<?>, ?> optionFunc) {
        final SchemaMeta schemaMeta;
        if (!(tableMeta instanceof VendorTableMeta)
                || (schemaMeta = tableMeta.schemaMeta()).isPseudoSchema()
                || schemaMeta.databaseMetadata() != this) {
            return Flux.error(MySQLExceptions.unknownTableMeta(tableMeta));
        }

        final StringBuilder builder = new StringBuilder(490);
        builder.append("SELECT it.INDEX_NAME,it.NON_UNIQUE,it.INDEX_TYPE,it.COLUMN_NAME")
                .append(",it.COLLATION,it.CARDINALITY,it.NULLABLE,it.IS_VISIBLE,it.INDEX_COMMENT,ct.COLUMN_KEY")
                .append(" FROM INFORMATION_SCHEMA.STATISTICS AS it JOIN INFORMATION_SCHEMA.COLUMNS AS ct")
                .append(" ON it.TABLE_SCHEMA = ct.TABLE_SCHEMA AND it.TABLE_NAME = ct.TABLE_NAME")
                .append(" AND it.COLUMN_NAME = ct.COLUMN_NAME")
                .append(" WHERE it.TABLE_SCHEMA = ");

        final boolean backslashEscapes;
        backslashEscapes = this.protocol.nonNullOf(Option.BACKSLASH_ESCAPES);

        MySQLStrings.appendLiteral(schemaMeta.schema(), backslashEscapes, builder);

        builder.append(" AND it.TABLE_NAME = ");

        MySQLStrings.appendLiteral(tableMeta.tableName(), backslashEscapes, builder);


        final Object nameValue, uniqueValue, typeValue;
        nameValue = optionFunc.apply(Option.NAME);
        if (nameValue instanceof String) {
            builder.append(" AND it.INDEX_NAME ");
            appendNamePredicate((String) nameValue, backslashEscapes, builder, UnaryOperator.identity());
        }

        uniqueValue = optionFunc.apply(Option.UNIQUE);
        if (uniqueValue instanceof Boolean) {
            builder.append(" AND it.NON_UNIQUE = ")
                    .append((Boolean) uniqueValue ? '0' : '1');
        }

        typeValue = optionFunc.apply(VendorOptions.INDEX_TYPE);
        if (typeValue instanceof String) {
            builder.append(" AND it.INDEX_TYPE ");
            appendNamePredicate((String) typeValue, backslashEscapes, builder, UnaryOperator.identity());
        }

        builder.append(" ORDER BY it.NON_UNIQUE,it.INDEX_NAME,it.SEQ_IN_INDEX");

        final AtomicReference<String> indexNameHolder = new AtomicReference<>(null);
        final Predicate<ResultRow> bufferPredicate;
        bufferPredicate = row -> {
            final String name;
            name = row.getNonNull("INDEX_NAME", String.class);
            return !name.equals(indexNameHolder.getAndSet(name));
        };


        final Function<List<ResultRow>, TableIndexMeta> mapBufferToIndexMetaFunc;
        mapBufferToIndexMetaFunc = rowList -> {
            // first, map columnList
            final List<IndexColumnMeta> columnList;
            columnList = mapToIndexColumnList(rowList);

            final ResultRow firstRow = rowList.get(0);
            final KeyType keyType;
            keyType = mapToKeyType(firstRow);

            final Map<Option<?>, Object> map = MySQLCollections.hashMap(8);

            map.put(VendorOptions.INDEX_TYPE, firstRow.getNonNull("INDEX_TYPE", String.class));
            map.put(VendorOptions.KEY_TYPE, keyType);
            map.put(Option.UNIQUE, keyType.isUnique() || !firstRow.getNonNull("NON_UNIQUE", Boolean.class));
            map.put(VendorOptions.COMMENT, firstRow.get("INDEX_COMMENT", String.class));

            return VendorTableIndexMeta.from(tableMeta, firstRow.getNonNull("INDEX_NAME", String.class), columnList, map);
        };

        final String sql = builder.toString();
        SqlLogger.printLog(optionFunc, sql);
        return this.protocol.query(Stmts.stmt(sql), CurrentRow.AS_RESULT_ROW, ResultStates.IGNORE_STATES)
                .bufferUntil(bufferPredicate, true)
                .map(mapBufferToIndexMetaFunc);
    }


    @Override
    public <R> Publisher<R> queryOption(final Option<R> option) {
        if (option != Option.USER) {
            return Mono.error(new JdbdException(String.format("don't support %s", option)));
        }
        final Function<CurrentRow, String> function;
        function = row -> row.getNonNull(0, String.class);
        return this.protocol.query(Stmts.stmt("SELECT CURRENT_USER()"), function, ResultStates.IGNORE_STATES)
                .last()
                .map(option.javaType()::cast);
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-keywords-table.html"> The INFORMATION_SCHEMA KEYWORDS Table</a>
     */
    @Override
    public Publisher<Map<String, Boolean>> sqlKeyWords(boolean onlyReserved) {
        final StringBuilder builder = new StringBuilder(80);
        builder.append("SELECT WORD,RESERVED FROM INFORMATION_SCHEMA.KEYWORDS");
        final int capacity;
        if (onlyReserved) {
            builder.append(" WHERE RESERVED = 1");
            capacity = (int) (262 / 0.75f);
        } else {
            capacity = (int) (752 / 0.75f);
        }
        // here,don't need ConcurrentMap,because function always run in netty EventLoop .
        final Map<String, Boolean> map = MySQLCollections.hashMap(capacity);
        final Function<CurrentRow, Boolean> function;
        function = row -> {
            map.put(row.getNonNull(0, String.class), row.getNonNull(1, Boolean.class));
            return Boolean.TRUE;
        };
        return this.protocol.query(Stmts.stmt(builder.toString()), function, ResultStates.IGNORE_STATES)
                .then(Mono.just(Collections.unmodifiableMap(map)));
    }

    @Override
    public String identifierQuoteString() {
        return String.valueOf(Constants.BACKTICK);
    }


    @Override
    public int sqlStateType() throws JdbdException {
        return DatabaseMetaData.SQL_STATE_SQL;
    }

    @Override
    public <T> T valueOf(Option<T> option) {
        return null;
    }

    @Override
    public Set<Option<?>> optionSet() {
        return Collections.emptySet();
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
     * @see #currentSchema(Function)
     * @see #queryTableMeta(SchemaMeta, Function)
     */
    private SchemaMeta mapSchema(CurrentRow row) {
        return VendorSchemaMeta.fromSchema(this, "def", row.getNonNull(0, String.class), Collections.emptyMap());
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
                    row.getNonNull(3, String.class), optionMap
            );
        };

        final String sql = builder.toString();
        SqlLogger.printLog(optionFunc, sql);
        return this.protocol.query(Stmts.stmt(sql), function, ResultStates.IGNORE_STATES);
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
        dataType = row.getNonNull("DATA_TYPE", String.class).toUpperCase(Locale.ROOT);
        columnType = row.getNonNull("COLUMN_TYPE", String.class).toUpperCase(Locale.ROOT);

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
        if (row.getNonNull("EXTRA", String.class).equalsIgnoreCase("auto_increment")) {
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
        if (row.getNonNull("EXTRA", String.class).toUpperCase(Locale.ROOT).contains("GENERATED")) {
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
        definition = row.get("COLUMN_TYPE", String.class);
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
                        throw new IllegalArgumentException(String.format("enum set don't support %s", clazz.getName()));
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
                enumSetFunc = clazz -> Collections.emptySet();
        }
        return enumSetFunc;
    }

    /**
     * @see #indexesOfTable(TableMeta, Function)
     * @see #mapToIndexColumnMeta(ResultRow)
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-statistics-table.html"> The INFORMATION_SCHEMA STATISTICS Table</a>
     */
    private List<IndexColumnMeta> mapToIndexColumnList(final List<ResultRow> rowList) {
        final List<IndexColumnMeta> columnList = MySQLCollections.arrayList(rowList.size());

        String lastIndexName = null, indexName;
        for (final ResultRow row : rowList) {
            indexName = row.getNonNull("INDEX_NAME", String.class);
            if (lastIndexName != null && !lastIndexName.equals(indexName)) {
                String m = String.format("index[%s] column row buffer error", indexName);
                throw new JdbdException(m);
            }
            lastIndexName = indexName;
            columnList.add(mapToIndexColumnMeta(row));
        }
        return columnList;
    }

    /**
     * @see #indexesOfTable(TableMeta, Function)
     * @see #mapToIndexColumnList(List)
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-statistics-table.html"> The INFORMATION_SCHEMA STATISTICS Table</a>
     */
    private IndexColumnMeta mapToIndexColumnMeta(final ResultRow row) {
        final Map<Option<?>, Object> map = MySQLCollections.hashMap(9);

        map.put(VendorOptions.CARDINALITY, row.getOrDefault("CARDINALITY", Long.class, 0L));

        switch (row.getOrDefault("COLLATION", String.class, "").toUpperCase(Locale.ROOT)) {
            case "A":
                map.put(VendorOptions.SORTING, Sorting.ASC);
                break;
            case "D":
                map.put(VendorOptions.SORTING, Sorting.DESC);
                break;
            default:
                map.put(VendorOptions.SORTING, Sorting.NONE);
        }

        map.put(VendorOptions.NULLS_SORTING, NullsSorting.UNKNOWN); // mysql don't support

        switch (row.getOrDefault("NULLABLE", String.class, "UNKNOWN").toUpperCase(Locale.ROOT)) {
            case "YES":
                map.put(VendorOptions.NULLABLE_MODE, BooleanMode.TRUE);
                break;
            case "":
            case "NO":
                map.put(VendorOptions.NULLABLE_MODE, BooleanMode.FALSE);
                break;
            default:
                map.put(VendorOptions.NULLABLE_MODE, BooleanMode.UNKNOWN);
        }

        map.put(VendorOptions.VISIBLE, row.getOrDefault("IS_VISIBLE", BooleanMode.class, BooleanMode.UNKNOWN));
        return VendorIndexColumnMeta.from(row.getNonNull("COLUMN_NAME", String.class), map);
    }

    /**
     * @param firstRow the first row that representing the first column of index.
     * @see #indexesOfTable(TableMeta, Function)
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-statistics-table.html"> The INFORMATION_SCHEMA STATISTICS Table</a>
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/information-schema-columns-table.html">The INFORMATION_SCHEMA COLUMNS Table</a>
     */
    private KeyType mapToKeyType(final ResultRow firstRow) {
        final KeyType keyType;
        switch (firstRow.getOrDefault("COLUMN_KEY", String.class, "").toUpperCase(Locale.ROOT)) {
            case "PRI":
                keyType = KeyType.PRIMARY_KEY;
                break;
            case "UNI":
                keyType = KeyType.UNIQUE_KEY;
                break;
            case "MUL": {
                switch (firstRow.getOrDefault("INDEX_TYPE", String.class, "").toUpperCase(Locale.ROOT)) {
                    case "FULLTEXT":
                        keyType = KeyType.FULL_TEXT_KEY;
                        break;
                    case "SPATIAL":
                        keyType = KeyType.SPATIAL_KEY;
                        break;
                    default:
                        keyType = KeyType.INDEX_KEY;
                }
            }
            break;
            default:
                keyType = KeyType.NONE;

        }
        return keyType;
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
