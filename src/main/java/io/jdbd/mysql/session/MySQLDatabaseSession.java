package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.MySQLType;
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
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.NamedSavePoint;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.task.PrepareTask;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
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
 *
 * </p>
 */
abstract class MySQLDatabaseSession<S extends DatabaseSession> extends MySQLSessionMetaSpec implements DatabaseSession {


    static final String COMMIT = "COMMIT";

    static final String ROLLBACK = "ROLLBACK";

    final MySQLDatabaseSessionFactory factory;

    private final AtomicInteger savePointIndex = new AtomicInteger(0);

    private final AtomicBoolean sessionClosed = new AtomicBoolean(false);

    MySQLDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        super(protocol);
        this.factory = factory;
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
        return this.executeQuery(sql, CurrentRow::asResultRow, DatabaseProtocol.IGNORE_RESULT_STATES);
    }

    @Override
    public final <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function) {
        return this.executeQuery(sql, function, DatabaseProtocol.IGNORE_RESULT_STATES);
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
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .thenReturn((S) this);
    }

    @Override
    public final Publisher<TransactionStatus> transactionStatus() {
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
        return Flux.from(this.protocol.staticMultiStmtAsFlux(Stmts.multiStmt(builder.toString())))
                .filter(ResultItem::isRowOrStatesItem)
                .collectList()
                .flatMap(this::mapTransactionStatus);
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
        return MySQLDatabaseMetadata.create(this.protocol);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */

    @Override
    public final Publisher<SavePoint> setSavePoint() {
        final StringBuilder builder;
        builder = MySQLStrings.builder()
                .append("jdbd-")
                .append(this.savePointIndex.getAndIncrement())
                .append('-')
                .append(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
        return this.setSavePoint(builder.toString(), DatabaseProtocol.OPTION_FUNC);
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @Override
    public final Publisher<SavePoint> setSavePoint(final String name) {
        return this.setSavePoint(name, DatabaseProtocol.OPTION_FUNC);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @Override
    public final Publisher<SavePoint> setSavePoint(final String name, final Function<Option<?>, ?> optionFunc) {
        if (!MySQLStrings.hasText(name)) {
            return Mono.error(MySQLExceptions.savePointNameIsEmpty());
        }
        final ParamStmt stmt;
        stmt = Stmts.single("SAVEPOINT ? ", MySQLType.VARCHAR, name);
        return this.protocol.paramUpdate(stmt, false)
                .thenReturn(NamedSavePoint.fromName(name));
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @Override
    public final Publisher<S> releaseSavePoint(final SavePoint savepoint) {
        return this.releaseSavePoint(savepoint, DatabaseProtocol.OPTION_FUNC);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Publisher<S> releaseSavePoint(SavePoint savepoint, Function<Option<?>, ?> optionFunc) {
        if (!(savepoint instanceof NamedSavePoint && MySQLStrings.hasText(savepoint.name()))) {
            return Mono.error(MySQLExceptions.unknownSavePoint(savepoint));
        }

        final ParamStmt stmt;
        stmt = Stmts.single("RELEASE SAVEPOINT ? ", MySQLType.VARCHAR, savepoint.name());
        return this.protocol.paramUpdate(stmt, false)
                .thenReturn((S) this);
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @Override
    public final Publisher<S> rollbackToSavePoint(final SavePoint savepoint) {
        return this.rollbackToSavePoint(savepoint, DatabaseProtocol.OPTION_FUNC);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/savepoint.html">SAVEPOINT</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Publisher<S> rollbackToSavePoint(SavePoint savepoint, Function<Option<?>, ?> optionFunc) {
        if (!(savepoint instanceof NamedSavePoint && MySQLStrings.hasText(savepoint.name()))) {
            return Mono.error(MySQLExceptions.unknownSavePoint(savepoint));
        }

        final ParamStmt stmt;
        stmt = Stmts.single("ROLLBACK TO SAVEPOINT ? ", MySQLType.VARCHAR, savepoint.name());
        return this.protocol.paramUpdate(stmt, false)
                .thenReturn((S) this);
    }


    @Override
    public final RefCursor refCursor(String name) {
        throw MySQLExceptions.dontSupportDeclareCursor(MySQLDriver.MY_SQL);
    }

    @Override
    public final RefCursor refCursor(String name, Function<Option<?>, ?> optionFunc) {
        throw MySQLExceptions.dontSupportDeclareCursor(MySQLDriver.MY_SQL);
    }

    @Override
    public final <T> T valueOf(Option<T> option) throws JdbdException {
        return this.protocol.valueOf(option);
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
    public final String toString() {
        final StringBuilder builder = new StringBuilder(290);

        builder.append(getClass().getName())
                .append("[ sessionIdentifier : ")
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
     * @see #transactionStatus()
     */
    abstract Mono<TransactionStatus> mapTransactionStatus(final List<ResultItem> list);

    /**
     * @see #toString()
     */
    abstract void printTransactionInfo(StringBuilder builder);


    /*################################## blow private method ##################################*/

    private PreparedStatement createPreparedStatement(final PrepareTask task) {
        return MySQLPreparedStatement.create(this, task);
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
