package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.SavePoint;
import io.jdbd.session.TransactionStatus;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.MultiStatement;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.StaticStatement;
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

    final MySQLDatabaseSessionFactory factory;

    private final AtomicInteger savePointIndex = new AtomicInteger(0);

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
        return this.protocol.threadId();
    }

    @Override
    public final Publisher<ResultStates> executeUpdate(final String sql) {
        if (!MySQLStrings.hasText(sql)) {
            return Mono.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.update(Stmts.stmt(sql));
    }

    @Override
    public final Publisher<ResultRow> executeQuery(String sql) {
        return this.executeQuery(sql, CurrentRow::asResultRow, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public final <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function) {
        return this.executeQuery(sql, function, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public final <R> Publisher<R> executeQuery(final String sql, final Function<CurrentRow, R> function,
                                               final Consumer<ResultStates> consumer) {
        if (!MySQLStrings.hasText(sql)) {
            return Flux.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.query(Stmts.stmt(sql, consumer), function);
    }

    @Override
    public final BatchQuery executeBatchQuery(final List<String> sqlGroup) {
        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.batchQueryError(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.batchQuery(Stmts.batch(sqlGroup));
    }

    @Override
    public final Publisher<ResultStates> executeBatchUpdate(final List<String> sqlGroup) {
        if (MySQLCollections.isEmpty(sqlGroup)) {
            return Flux.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.batchUpdate(Stmts.batch(sqlGroup));
    }

    @Override
    public final MultiResult executeBatchAsMulti(final List<String> sqlGroup) {
        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.multiError(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.batchAsMulti(Stmts.batch(sqlGroup));
    }

    @Override
    public final OrderedFlux executeBatchAsFlux(final List<String> sqlGroup) {
        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.fluxError(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.batchAsFlux(Stmts.batch(sqlGroup));
    }

    @Override
    public final OrderedFlux executeAsFlux(final String multiStmt) {
        if (!MySQLStrings.hasText(multiStmt)) {
            return MultiResults.fluxError(MySQLExceptions.sqlIsEmpty());
        }
        return this.protocol.executeAsFlux(Stmts.multiStmt(multiStmt));
    }

    @Override
    public final Publisher<RefCursor> declareCursor(String sql) {
        return Mono.error(MySQLExceptions.dontSupportDeclareCursor(MySQLDriver.MY_SQL));
    }

    @Override
    public final Publisher<TransactionStatus> transactionStatus() {
        return this.protocol.transactionStatus();
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
        if (this.protocol.isClosed()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
        return MySQLDatabaseMetadata.create(this.protocol);
    }


    @Override
    public final Publisher<SavePoint> setSavePoint() {
        final StringBuilder builder;
        builder = MySQLStrings.builder()
                .append("$jdbd-")
                .append(this.savePointIndex.getAndIncrement())
                .append('-')
                .append(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
        return this.setSavePoint(builder.toString());
    }

    @Override
    public final Publisher<SavePoint> setSavePoint(final String name) {
        if (!MySQLStrings.hasText(name)) {
            return Mono.error(MySQLExceptions.savePointNameIsEmpty());
        }
        final ParamStmt stmt;
        stmt = Stmts.single("SAVEPOINT ? ", MySQLType.VARCHAR, name);
        return this.protocol.paramUpdate(stmt, false)
                .thenReturn(NamedSavePoint.fromName(name));
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Publisher<S> releaseSavePoint(final SavePoint savepoint) {
        if (!(savepoint instanceof NamedSavePoint && MySQLStrings.hasText(savepoint.name()))) {
            return Mono.error(MySQLExceptions.unknownSavePoint(savepoint));
        }

        final ParamStmt stmt;
        stmt = Stmts.single("RELEASE SAVEPOINT ? ", MySQLType.VARCHAR, savepoint.name());
        return this.protocol.paramUpdate(stmt, false)
                .thenReturn((S) this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Publisher<S> rollbackToSavePoint(final SavePoint savepoint) {
        if (!(savepoint instanceof NamedSavePoint && MySQLStrings.hasText(savepoint.name()))) {
            return Mono.error(MySQLExceptions.unknownSavePoint(savepoint));
        }

        final ParamStmt stmt;
        stmt = Stmts.single("ROLLBACK TO SAVEPOINT ? ", MySQLType.VARCHAR, savepoint.name());
        return this.protocol.paramUpdate(stmt, false)
                .thenReturn((S) this);
    }

    @Override
    public final boolean isClosed() {
        return this.protocol.isClosed();
    }


    @Override
    public final boolean isSameFactory(DatabaseSession session) {
        return session instanceof MySQLDatabaseSession && ((MySQLDatabaseSession<?>) session).factory == this.factory;
    }


    @Override
    public final <T> Publisher<T> close() {
        return this.protocol.close();
    }



    /*################################## blow private method ##################################*/

    private PreparedStatement createPreparedStatement(final PrepareTask task) {
        return MySQLPreparedStatement.create(this, task);
    }


}
