package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.stmt.PrepareStmtTask;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.util.function.Consumer;

public final class ServerPreparedStatement<S extends MySQLDatabaseSession> extends MySQLStatement<S>
        implements PreparedStatement {

    public static <S extends MySQLDatabaseSession> ServerPreparedStatement<S> create(S session, PrepareStmtTask task) {
        return new ServerPreparedStatement<>(session, task);
    }

    final S session;

    final PrepareStmtTask task;

    private ServerPreparedStatement(S session, PrepareStmtTask task) {
        super(session);
        this.session = session;
        this.task = task;
    }


    @Override
    public void bind(int indexBasedZero, JDBCType jdbcType, Object nullable) throws JdbdException {

    }

    @Override
    public void bind(int indexBasedZero, SQLType sqlType, Object nullable) throws JdbdException {

    }

    @Override
    public final void bind(int indexBasedZero, @Nullable Object nullable) {

    }

    @Override
    public void addBatch() {

    }

    @Override
    public boolean setFetchSize(int fetchSize) {
        return false;
    }

    @Override
    public boolean supportPublisher() {
        return false;
    }

    @Override
    public boolean supportOutParameter() {
        return false;
    }

    @Override
    public final Flux<ResultStates> executeBatch() {
        return null;
    }

    @Override
    public Mono<ResultStates> executeUpdate() {
        return null;
    }

    @Override
    public Flux<ResultRow> executeQuery() {
        return null;
    }

    @Override
    public Flux<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer) {
        return null;
    }

    @Override
    public ReactorMultiResult executeBatchAsMulti() {
        return null;
    }


    @Override
    public Flux<Result> executeBatchAsFlux() {
        return null;
    }
}
