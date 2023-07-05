package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.DataType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.AttrPreparedStatement;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.ResultType;
import io.jdbd.statement.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.JdbdParamValue;
import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.task.PrepareTask;
import io.jdbd.vendor.util.JdbdBinds;
import io.jdbd.vendor.util.JdbdFunctions;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This interface is a implementation of {@link PreparedStatement} with MySQL client protocol.
 * </p>
 */
final class MySQLPreparedStatement extends MySQLStatement implements AttrPreparedStatement {

    static MySQLPreparedStatement create(MySQLDatabaseSession session, PrepareTask<MySQLType> task) {
        return new MySQLPreparedStatement(session, task);
    }

    private final String sql;

    private final PrepareTask<MySQLType> stmtTask;

    private final List<MySQLType> paramTypes;

    private final ResultRowMeta rowMeta;

    private final Warning warning;

    private final int paramCount;

    private final List<List<ParamValue>> paramGroupList = new ArrayList<>();

    private List<ParamValue> paramGroup;

    private MySQLPreparedStatement(final MySQLDatabaseSession session, final PrepareTask<MySQLType> stmtTask) {
        super(session);
        this.sql = stmtTask.getSql();
        this.stmtTask = stmtTask;
        this.paramTypes = stmtTask.getParamTypes();
        this.rowMeta = stmtTask.getRowMeta();

        this.warning = stmtTask.getWarning();
        this.paramCount = this.paramTypes.size();

        if (this.paramCount == 0) {
            this.paramGroup = Collections.emptyList();
        } else {
            this.paramGroup = new ArrayList<>(this.paramCount);
        }

    }

    @Override
    public PreparedStatement bind(final int indexBasedZero, final @Nullable Object nullable) throws JdbdException {
        checkReuse();
        final List<ParamValue> paramGroup = this.paramGroup;
        if (indexBasedZero < 0 || indexBasedZero >= this.paramCount) {
            final JdbdException error;
            error = MySQLExceptions.invalidParameterValue(this.paramGroupList.size(), indexBasedZero);
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }
        paramGroup.add(JdbdParamValue.wrap(indexBasedZero, nullable));
        return this;
    }

    @Override
    public PreparedStatement bind(int indexBasedZero, JDBCType jdbcType,@Nullable Object nullable) throws JdbdException {
        return this;
    }

    @Override
    public PreparedStatement bind(int indexBasedZero, DataType dataType,@Nullable Object nullable) throws JdbdException {
        return this;
    }

    @Override
    public PreparedStatement bind(int indexBasedZero, String dataTypeName, @Nullable Object nullable) throws JdbdException {
        return this;
    }

    @Override
    public PreparedStatement addBatch() {
        final List<ParamValue> paramGroup = this.paramGroup;
        final int paramCount = this.paramCount;

        // add bind group
        final JdbdException error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup.size() != paramCount) {
            error = MySQLExceptions.parameterCountMatch(this.paramGroupList.size()
                    , this.paramCount, paramGroup.size());
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(this.paramGroupList.size(), paramGroup);
        }

        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }
        this.paramGroupList.add(paramGroup);
        if (paramCount > 0) {
            this.paramGroup = new ArrayList<>(paramCount);
        }
        return this;
    }


    @Override
    public List<? extends DataType> getParamTypeList() {
        return null;
    }

    @Override
    public Mono<ResultStates> executeUpdate() {
        final List<ParamValue> paramGroup = this.paramGroup;

        final Throwable error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.QUERY);
        } else if (this.paramGroupList.size() > 0) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH);
        } else if (paramGroup.size() != this.paramCount) {
            error = MySQLExceptions.parameterCountMatch(0, this.paramCount, paramGroup.size());
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final Mono<ResultStates> mono;
        if (error == null) {
            this.statementOption.fetchSize = 0;
            ParamStmt stmt = Stmts.paramStmt(this.sql, paramGroup, this.statementOption);
            mono = this.stmtTask.executeUpdate(stmt);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            mono = Mono.error(error);
        }
        clearStatementToAvoidReuse();
        return mono;
    }

    @Override
    public Flux<ResultRow> executeQuery() {
        return executeQuery(JdbdFunctions.noActionConsumer());
    }

    @Override
    public Flux<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer) {
        Objects.requireNonNull(statesConsumer, "statesConsumer");

        final List<ParamValue> paramGroup = this.paramGroup;

        final Throwable error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta == null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.UPDATE);
        } else if (this.paramGroupList.size() > 0) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH);
        } else if (paramGroup.size() != this.paramCount) {
            error = MySQLExceptions.parameterCountMatch(0, this.paramCount, paramGroup.size());
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final Flux<ResultRow> flux;
        if (error == null) {
            final ParamStmt stmt;
            stmt = Stmts.paramStmt(this.sql, paramGroup, statesConsumer, this.statementOption);
            flux = this.stmtTask.executeQuery(stmt);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public <R> Publisher<R> executeQuery(Function<CurrentRow, R> function) {
        return AttrPreparedStatement.super.executeQuery(function);
    }

    @Override
    public <R> Publisher<R> executeQuery(Function<CurrentRow, R> function, Consumer<ResultStates> statesConsumer) {
        return AttrPreparedStatement.super.executeQuery(function, statesConsumer);
    }

    @Override
    public Flux<ResultStates> executeBatchUpdate() {
        final Flux<ResultStates> flux;
        if (this.paramGroup == null) {
            flux = Flux.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
        } else if (this.paramGroupList.size() == 0) {
            final JdbdException error = MySQLExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        } else {
            final ParamBatchStmt<ParamValue> stmt;
            stmt = Stmts.paramBatch(this.sql, this.paramGroupList, this.statementOption);
            flux = this.stmtTask.executeBatch(stmt);
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        final MultiResult result;
        if (this.paramGroup == null) {
            result = MultiResults.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
        } else if (this.paramGroupList.size() == 0) {
            final JdbdException error = MySQLExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            result = MultiResults.error(error);
        } else {
            this.statementOption.fetchSize = 0; // executeBatchAsMulti() don't support fetch.
            final ParamBatchStmt<ParamValue> stmt;
            stmt = Stmts.paramBatch(this.sql, this.paramGroupList, this.statementOption);
            result = this.stmtTask.executeBatchAsMulti(stmt);
        }
        clearStatementToAvoidReuse();
        return result;
    }


    @Override
    public OrderedFlux executeBatchAsFlux() {
        final OrderedFlux flux;
        if (this.paramGroup == null) {
            flux = MultiResults.fluxError(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
        } else if (this.paramGroupList.size() == 0) {
            final JdbdException error = MySQLExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = MultiResults.fluxError(error);
        } else {
            final ParamBatchStmt<ParamValue> stmt;
            stmt = Stmts.paramBatch(this.sql, this.paramGroupList, this.statementOption);
            flux = this.stmtTask.executeBatchAsFlux(stmt);
        }
        clearStatementToAvoidReuse();
        return flux;
    }


    @Override
    public boolean setFetchSize(final int fetchSize) {
        this.statementOption.fetchSize = fetchSize;
        return fetchSize > 0;
    }

    @Override
    public boolean supportPublisher() {
        // always true,ComPrepare
        return true;
    }

    @Override
    public boolean supportOutParameter() {
        // always true,out parameter return as extra Result.
        return true;
    }

    @Override
    public Publisher<DatabaseSession> abandonBind() {
        return this.stmtTask.abandonBind()
                .thenReturn(this.session);
    }


    @Override
    public Warning getWaring() {
        return this.warning;
    }


    /*################################## blow packet template method ##################################*/

    @Override
    void checkReuse() throws JdbdSQLException {
        if (this.paramGroup == null) {
            throw MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        }
    }

    @Override
    void closeOnBindError(Throwable error) {
        this.stmtTask.closeOnBindError(error);
    }

    /*################################## blow private method ##################################*/


    private void clearStatementToAvoidReuse() {
        this.paramGroup = null;
    }


}