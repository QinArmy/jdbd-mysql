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

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.*;
import io.jdbd.session.*;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.MultiStatement;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.StaticStatement;
import io.jdbd.util.NameMode;
import io.jdbd.util.SqlLogger;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.NamedSavePoint;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.task.PrepareTask;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * <p>
 * This class is a implementation of {@link DatabaseSession} with MySQL protocol.
 * This class is base class of below class:
 * <ul>
 *     <li>{@link MySQLLocalDatabaseSession}</li>
 *     <li>{@link MySQLRmDatabaseSession}</li>
 * </ul>
 * <p>
 * <br/>
 */
abstract class MySQLDatabaseSession<S extends DatabaseSession> extends MySQLSessionMetaSpec implements DatabaseSession {


    static final String COMMIT = "COMMIT";

    static final String ROLLBACK = "ROLLBACK";

    final String name;

    final MySQLDatabaseSessionFactory factory;

    private final AtomicInteger savePointIndex = new AtomicInteger(0);

    private final AtomicBoolean sessionClosed = new AtomicBoolean(false);

    MySQLDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol, String name) {
        super(protocol);
        this.name = name;
        this.factory = factory;
    }

    @Override
    public final String name() {
        return this.name;
    }

    @Override
    public final String factoryName() {
        return this.factory.name();
    }

    @Override
    public final long sessionIdentifier() {
        if (this.isClosed()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
        return this.protocol.sessionIdentifier();
    }

    @Override
    public final Publisher<ResultStates> executeUpdate(final String sql) {
        if (!MySQLStrings.hasText(sql)) {
            return Mono.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.update(Stmts.stmtWithSession(sql, this));
    }

    @Override
    public final Publisher<ResultRow> executeQuery(String sql) {
        return this.executeQuery(sql, CurrentRow.AS_RESULT_ROW, ResultStates.IGNORE_STATES);
    }

    @Override
    public final <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function) {
        return this.executeQuery(sql, function, ResultStates.IGNORE_STATES);
    }

    @Override
    public final <R> Publisher<R> executeQuery(final String sql, final Function<CurrentRow, R> function,
                                               final Consumer<ResultStates> consumer) {
        if (!MySQLStrings.hasText(sql)) {
            return Flux.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.query(Stmts.stmtWithSession(sql, this), function, consumer);
    }


    @Override
    public final QueryResults executeBatchQuery(final List<String> sqlGroup) {
        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.batchQueryError(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.batchQuery(Stmts.batchWithSession(sqlGroup, this));
    }

    @Override
    public final Publisher<ResultStates> executeBatchUpdate(final List<String> sqlGroup) {
        if (MySQLCollections.isEmpty(sqlGroup)) {
            return Flux.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.batchUpdate(Stmts.batchWithSession(sqlGroup, this));
    }

    @Override
    public final MultiResult executeBatchAsMulti(final List<String> sqlGroup) {
        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.multiError(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.batchAsMulti(Stmts.batchWithSession(sqlGroup, this));
    }

    @Override
    public final OrderedFlux executeBatchAsFlux(final List<String> sqlGroup) {
        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.fluxError(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.batchAsFlux(Stmts.batchWithSession(sqlGroup, this));
    }


    @Override
    public final OrderedFlux executeMultiStmt(final String multiStmt) {
        if (!MySQLStrings.hasText(multiStmt)) {
            return MultiResults.fluxError(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.staticMultiStmtAsFlux(Stmts.multiStmtWithSession(multiStmt, this));
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-transaction.html">SET TRANSACTION Statement</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Publisher<S> setTransactionCharacteristics(final @Nullable TransactionOption option) {
        if (option == null) {
            return Mono.error(new NullPointerException());
        }
        final StringBuilder builder = new StringBuilder(30);
        builder.append("SET SESSION TRANSACTION ");

        final Isolation isolation;
        isolation = option.isolation();
        if (isolation != null) {
            builder.append("ISOLATION LEVEL ");
            if (appendIsolation(isolation, builder)) {
                return Mono.error(MySQLExceptions.unknownIsolation(isolation));
            }
            builder.append(Constants.SPACE_COMMA_SPACE);
        }
        if (option.isReadOnly()) {
            builder.append("READ ONLY");
        } else {
            builder.append("READ WRITE");
        }
        final String sql;
        sql = builder.toString();

        SqlLogger.printLog(option::valueOf, sql);
        return this.protocol.update(Stmts.stmt(sql))
                .thenReturn((S) this);
    }

    @Override
    public final Publisher<TransactionInfo> transactionInfo() {
        return transactionInfo(Option.EMPTY_OPTION_FUNC);
    }

    @Override
    public final Publisher<TransactionInfo> transactionInfo(final Function<Option<?>, ?> optionFunc) {
        final TransactionInfo info;
        info = obtainTransactionInfo();
        if (info != null) {
            return Mono.just(info);
        }
        return sessionTransactionCharacteristics(optionFunc)
                .doOnSuccess(v -> {
                    if (this.protocol.inTransaction()) {
                        String m = "Not found cache current transaction info,you don't use jdbd-spi to control transaction.";
                        throw new JdbdException(m);
                    }
                });
    }


    @Override
    public final Publisher<TransactionInfo> sessionTransactionCharacteristics() {
        return sessionTransactionCharacteristics(Option.EMPTY_OPTION_FUNC);
    }

    @Override
    public final Mono<TransactionInfo> sessionTransactionCharacteristics(final Function<Option<?>, ?> optionFunc) {
        final ServerVersion version = this.protocol.serverVersion();
        final StringBuilder builder = new StringBuilder(139);
        if (version.meetsMinimum(8, 0, 3)
                || (version.meetsMinimum(5, 7, 20) && !version.meetsMinimum(8, 0, 0))) {
            builder.append("SELECT @@session.transaction_isolation AS txLevel")
                    .append(",@@session.transaction_read_only AS txReadOnly");
        } else {
            builder.append("SELECT @@session.tx_isolation AS txLevel")
                    .append(",@@session.tx_read_only AS txReadOnly");
        }

        final String sql;
        sql = builder.toString();

        SqlLogger.printLog(optionFunc, sql);
        return Flux.from(this.protocol.query(Stmts.stmt(sql), this::mapToSessioinTransactionInfo, ResultStates.IGNORE_STATES))
                .last();
    }

    @Override
    public final boolean inTransaction() throws JdbdException {
        if (this.isClosed()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
        return this.protocol.inTransaction();
    }

    @Override
    public final StaticStatement statement() {
        return MySQLStaticStatement.create(this);
    }

    @Override
    public final Publisher<PreparedStatement> prepareStatement(final String sql) {
        if (!MySQLStrings.hasText(sql)) {
            return Mono.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.prepare(sql)
                .map(this::createPreparedStatement);
    }

    @Override
    public final BindStatement bindStatement(final String sql) {
        return this.bindStatement(sql, false);
    }

    @Override
    public final BindStatement bindStatement(final String sql, final boolean forceServerPrepared) {
        if (!MySQLStrings.hasText(sql)) {
            throw MySQLExceptions.bindSqlHaveNoText();
        }
        return MySQLBindStatement.create(this, sql, forceServerPrepared);
    }

    @Override
    public final MultiStatement multiStatement() throws JdbdException {
        if (!this.protocol.supportMultiStmt()) {
            throw MySQLExceptions.dontSupportMultiStmt();
        }
        return MySQLMultiStatement.create(this);
    }

    @Override
    public final DatabaseMetaData databaseMetaData() throws JdbdException {
        if (this.isClosed()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
        return MySQLDatabaseMetadata.create(this);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @Override
    public final Publisher<SavePoint> setSavePoint() {
        return this.setSavePoint(Option.EMPTY_OPTION_FUNC);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @Override
    public final Publisher<SavePoint> setSavePoint(final Function<Option<?>, ?> optionFunc) {
        String name;
        if (optionFunc == Option.EMPTY_OPTION_FUNC || (name = (String) optionFunc.apply(Option.NAME)) == null) {
            name = MySQLStrings.builder()
                    .append("jdbd_")
                    .append(this.savePointIndex.getAndIncrement())
                    .append('_')
                    .append(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE))
                    .toString();
        }

        if (!MySQLStrings.hasText(name)) {
            return Mono.error(MySQLExceptions.savePointNameIsEmpty());
        }
        final StringBuilder builder = new StringBuilder(12 + name.length());
        builder.append("SAVEPOINT ");

        final RuntimeException error;
        if ((error = MySQLStrings.appendMySqlIdentifier(name, builder)) != null) {
            return Mono.error(error);
        }
        final String sql;
        sql = builder.toString();

        SqlLogger.printLog(optionFunc, sql);
        return this.protocol.update(Stmts.stmt(sql))
                .thenReturn(NamedSavePoint.fromName(name));
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @Override
    public final Publisher<S> releaseSavePoint(final SavePoint savepoint) {
        return this.releaseSavePoint(savepoint, Option.EMPTY_OPTION_FUNC);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Publisher<S> releaseSavePoint(SavePoint savepoint, Function<Option<?>, ?> optionFunc) {
        final String name;
        if (!(savepoint instanceof NamedSavePoint && MySQLStrings.hasText(name = savepoint.name()))) {
            return Mono.error(MySQLExceptions.unknownSavePoint(savepoint));
        }

        final StringBuilder builder = new StringBuilder(22 + name.length());
        builder.append("RELEASE SAVEPOINT ");

        final RuntimeException error;
        if ((error = MySQLStrings.appendMySqlIdentifier(name, builder)) != null) {
            return Mono.error(error);
        }
        final String sql;
        sql = builder.toString();

        SqlLogger.printLog(optionFunc, sql);
        return this.protocol.update(Stmts.stmt(sql))
                .thenReturn((S) this);
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @Override
    public final Publisher<S> rollbackToSavePoint(final SavePoint savepoint) {
        return this.rollbackToSavePoint(savepoint, Option.EMPTY_OPTION_FUNC);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Publisher<S> rollbackToSavePoint(SavePoint savepoint, Function<Option<?>, ?> optionFunc) {
        final String name;
        if (!(savepoint instanceof NamedSavePoint && MySQLStrings.hasText(name = savepoint.name()))) {
            return Mono.error(MySQLExceptions.unknownSavePoint(savepoint));
        }

        final StringBuilder builder = new StringBuilder(25 + name.length());
        builder.append("ROLLBACK TO SAVEPOINT ");

        final RuntimeException error;
        if ((error = MySQLStrings.appendMySqlIdentifier(name, builder)) != null) {
            return Mono.error(error);
        }
        final String sql;
        sql = builder.toString();

        SqlLogger.printLog(optionFunc, sql);
        return this.protocol.update(Stmts.stmt(sql))
                .thenReturn((S) this);
    }


    @Override
    public final Cursor refCursor(String name) {
        throw MySQLExceptions.dontSupportDeclareCursor(MySQLDriver.MY_SQL);
    }

    @Override
    public final Cursor refCursor(String name, Function<Option<?>, ?> optionFunc) {
        throw MySQLExceptions.dontSupportDeclareCursor(MySQLDriver.MY_SQL);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/string-type-syntax.html">TEXT</a>
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/string-literals.html#character-escape-sequences"> Special Character Escape Sequences</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final S appendLiteral(@Nullable String text, StringBuilder builder) throws JdbdException {
        if (isClosed()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
        MySQLStrings.appendLiteral(text, this.protocol.nonNullOf(Option.BACKSLASH_ESCAPES), builder);
        return (S) this;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/identifiers.html">Schema Object Names</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final S appendIdentifier(String identifier, StringBuilder builder) throws JdbdException {
        final RuntimeException error;
        if ((error = MySQLStrings.appendMySqlIdentifier(identifier, builder)) != null) {
            throw error;
        }
        return (S) this;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/identifiers.html">Schema Object Names</a>
     * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html">Identifier Case Sensitivity</a>
     * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_lower_case_table_names">lower_case_table_names</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final S appendTableName(String tableName, NameMode mode, StringBuilder builder) throws JdbdException {
        MySQLStrings.appendTableNameOrColumnName(tableName, mode, builder);
        return (S) this;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/identifiers.html">Schema Object Names</a>
     * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html">Identifier Case Sensitivity</a>
     * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_lower_case_table_names">lower_case_table_names</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final S appendColumnName(String columnName, NameMode mode, StringBuilder builder) throws JdbdException {
        MySQLStrings.appendTableNameOrColumnName(columnName, mode, builder);
        return (S) this;
    }

    @Override
    public final <T> T valueOf(Option<T> option) throws JdbdException {
        return this.protocol.valueOf(option);
    }


    @Override
    public final Set<Option<?>> optionSet() {
        return this.protocol.optionSet();
    }

    @Override
    public final boolean isSameFactory(DatabaseSession session) {
        return session instanceof MySQLDatabaseSession
                && ((MySQLDatabaseSession<?>) session).factory == this.factory;
    }


    @Override
    public final <T> Publisher<T> close() {
        return Mono.defer(this::closeSession);
    }

    @Override
    public final boolean isClosed() {
        return this.sessionClosed.get() || this.protocol.isClosed();
    }

    @Override
    public final int hashCode() {
        return super.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        return obj == this;
    }

    @Override
    public final String toString() {
        final StringBuilder builder = new StringBuilder(290);

        builder.append(getClass().getName())
                .append("[ name : ")
                .append(this.name)
                .append(", sessionIdentifier : ")
                .append(this.protocol.sessionIdentifier())
                .append(" , factoryName : ")
                .append(this.factory.name())
                .append(" , factoryVendor : ")
                .append(this.factory.factoryVendor())
                .append(" , driverVendor : ")
                .append(this.driverVendor())
                .append(" , serverVersion : ")
                .append(this.protocol.serverVersion().getVersion())
                .append(" , driverVersion : ")
                .append(MySQLDriver.getInstance().version().getVersion());

        printTransactionInfo(builder);

        return builder.append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


    /**
     * <p>Get current transaction session
     *
     * @see #transactionInfo()
     */
    @Nullable
    abstract TransactionInfo obtainTransactionInfo();

    /**
     * @see #toString()
     */
    abstract void printTransactionInfo(StringBuilder builder);

    void onSessionClose() {
        if (this.protocol.isClosed()) {
            this.sessionClosed.set(true);
        }
    }




    /*################################## blow private method ##################################*/

    private PreparedStatement createPreparedStatement(final PrepareTask task) {
        return MySQLPreparedStatement.create(this, task);
    }


    /**
     * @see #sessionTransactionCharacteristics(Function)
     */
    private TransactionInfo mapToSessioinTransactionInfo(final CurrentRow row) {
        return TransactionInfo.notInTransaction(row.getNonNull(0, Isolation.class), row.getNonNull(1, Boolean.class));
    }


    /**
     * @see #close()
     */
    private <T> Mono<T> closeSession() {
        final Mono<T> mono;
        if (this.sessionClosed.compareAndSet(false, true)) {
            mono = this.protocol.close();
        } else {
            mono = Mono.empty();
        }
        return mono;
    }


    static boolean appendIsolation(final Isolation isolation, final StringBuilder builder) {

        boolean error = false;
        if (isolation == Isolation.READ_COMMITTED) {
            builder.append("READ COMMITTED");
        } else if (isolation == Isolation.REPEATABLE_READ) {
            builder.append("REPEATABLE READ");
        } else if (isolation == Isolation.SERIALIZABLE) {
            builder.append("SERIALIZABLE");
        } else if (isolation == Isolation.READ_UNCOMMITTED) {
            builder.append("READ UNCOMMITTED");
        } else {
            error = true;
        }
        return error;
    }


}
