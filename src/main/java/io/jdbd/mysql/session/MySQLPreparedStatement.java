package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.vendor.ResultType;
import io.jdbd.vendor.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.task.PrepareTask;
import io.jdbd.vendor.util.JdbdBinds;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.jdbd.mysql.MySQLDriver.MY_SQL;

/**
 * <p>
 * This interface is a implementation of {@link PreparedStatement} with MySQL client protocol.
 * </p>
 *
 * @since 1.0
 */
final class MySQLPreparedStatement extends MySQLStatement<PreparedStatement> implements PreparedStatement {

    static MySQLPreparedStatement create(MySQLDatabaseSession<?> session, PrepareTask task) {
        return new MySQLPreparedStatement(session, task);
    }

    private final String sql;

    private final PrepareTask stmtTask;

    private final List<? extends DataType> paramTypes;

    private final ResultRowMeta rowMeta;

    private final Warning warning;

    private final int paramCount;

    private List<List<ParamValue>> paramGroupList;

    private List<ParamValue> paramGroup;

    private MySQLPreparedStatement(final MySQLDatabaseSession<?> session, final PrepareTask stmtTask) {
        super(session);
        this.sql = stmtTask.getSql();
        this.stmtTask = stmtTask;
        this.paramTypes = stmtTask.getParamTypes();
        this.rowMeta = stmtTask.getRowMeta();

        this.warning = stmtTask.getWarning();
        this.paramCount = this.paramTypes.size();
    }

    @Override
    public ResultRowMeta resultRowMeta() {
        return this.rowMeta;
    }

    @Override
    public List<? extends DataType> paramTypeList() {
        return this.paramTypes;
    }

    @Override
    public Warning waring() {
        return this.warning;
    }


    @Override
    public PreparedStatement bind(final int indexBasedZero, final @Nullable DataType dataType,
                                  final @Nullable Object value) throws JdbdException {

        List<ParamValue> paramGroup = this.paramGroup;

        if (paramGroup == EMPTY_PARAM_GROUP) {
            throw MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        }

        final int paramCount = this.paramCount;
        final RuntimeException error;
        final MySQLType type;

        if (indexBasedZero < 0 || indexBasedZero == paramCount) {
            List<List<ParamValue>> paramGroupList = this.paramGroupList;
            final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();
            error = MySQLExceptions.invalidParameterValue(groupSize, indexBasedZero);
        } else if (dataType == null) {
            error = MySQLExceptions.dataTypeIsNull();
        } else if (value != null && (dataType == JdbdType.NULL || dataType == MySQLType.NULL)) {
            error = MySQLExceptions.nonNullBindValueOf(dataType);
        } else if ((type = MySQLBinds.mapDataType(dataType)) == null) {
            error = MySQLExceptions.dontSupportDataType(dataType, MY_SQL);
        } else {
            error = null;
            if (paramGroup == null) {
                this.paramGroup = paramGroup = MySQLCollections.arrayList(paramCount);
            }
            paramGroup.add(JdbdValues.paramValue(indexBasedZero, type, value));
        }

        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            clearStatementToAvoidReuse();
            throw error;
        }
        return this;
    }


    @Override
    public PreparedStatement addBatch() {
        final int paramCount = this.paramCount;
        final List<ParamValue> paramGroup = this.paramGroup;
        final int paramSize = paramGroup == null ? 0 : paramGroup.size();

        List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();


        // add bind group
        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramSize != paramCount) {
            error = MySQLExceptions.parameterCountMatch(groupSize, paramCount, paramSize);
        } else {
            if (paramGroupList == null) {
                this.paramGroupList = paramGroupList = MySQLCollections.arrayList();
            }
            if (paramGroup == null) {
                error = null;
                paramGroupList.add(EMPTY_PARAM_GROUP);
            } else {
                error = JdbdBinds.sortAndCheckParamGroup(groupSize, paramGroup);
                paramGroupList.add(MySQLCollections.unmodifiableList(paramGroup));
            }
        }

        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            clearStatementToAvoidReuse();
            throw MySQLExceptions.wrap(error);
        }

        this.paramGroup = null; // clear for next batch
        return this;
    }


    @Override
    public Publisher<ResultStates> executeUpdate() {
        this.endStmtOption();

        List<ParamValue> paramGroup = this.paramGroup;
        final int paramSize = paramGroup == null ? 0 : paramGroup.size();

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.QUERY);
        } else if (this.paramGroupList != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH_UPDATE);
        } else if (paramSize != this.paramCount) {
            error = MySQLExceptions.parameterCountMatch(0, this.paramCount, paramSize);
        } else if (paramGroup == null) {
            error = null;
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final Mono<ResultStates> mono;
        if (error == null) {
            this.fetchSize = 0;
            mono = this.stmtTask.executeUpdate(Stmts.paramStmt(this.sql, paramGroup, this));
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            mono = Mono.error(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return mono;
    }

    @Override
    public Publisher<ResultRow> executeQuery() {
        return this.executeQuery(CurrentRow::asResultRow, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(Function<CurrentRow, R> function) {
        return this.executeQuery(function, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(final @Nullable Function<CurrentRow, R> function,
                                         final @Nullable Consumer<ResultStates> consumer) {
        this.endStmtOption();

        List<ParamValue> paramGroup = this.paramGroup;
        final int paramSize = paramGroup == null ? 0 : paramGroup.size();

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta == null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.UPDATE);
        } else if (this.paramGroupList != null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH_UPDATE);
        } else if (paramSize != this.paramCount) {
            error = MySQLExceptions.parameterCountMatch(0, this.paramCount, paramSize);
        } else if (function == null) {
            error = MySQLExceptions.queryMapFuncIsNull();
        } else if (consumer == null) {
            error = MySQLExceptions.statesConsumerIsNull();
        } else if (paramGroup == null) {
            error = null;
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final Flux<R> flux;
        if (error == null) {
            flux = this.stmtTask.executeQuery(Stmts.paramStmt(this.sql, paramGroup, consumer, this), function);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public BatchQuery executeBatchQuery() {
        this.endStmtOption();


        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else if (this.rowMeta == null) {
            error = new SubscribeException(ResultType.BATCH_QUERY, ResultType.UPDATE);
        } else {
            error = null;
        }

        final BatchQuery batchQuery;
        if (error == null) {
            batchQuery = this.stmtTask.executeBatchQuery(Stmts.paramBatch(this.sql, paramGroupList, this));
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            batchQuery = MultiResults.batchQueryError(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return batchQuery;
    }

    @Override
    public Publisher<ResultStates> executeBatchUpdate() {
        this.endStmtOption();


        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else if (this.rowMeta != null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH_UPDATE);
        } else {
            error = null;
        }

        final Flux<ResultStates> flux;
        if (error == null) {
            flux = this.stmtTask.executeBatchUpdate(Stmts.paramBatch(this.sql, paramGroupList, this));
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        this.endStmtOption();

        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }

        final MultiResult multiResult;
        if (error == null) {
            multiResult = this.stmtTask.executeBatchAsMulti(Stmts.paramBatch(this.sql, paramGroupList, this));
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            multiResult = MultiResults.multiError(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return multiResult;
    }


    @Override
    public OrderedFlux executeBatchAsFlux() {
        this.endStmtOption();

        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }

        final OrderedFlux flux;
        if (error == null) {
            flux = this.stmtTask.executeBatchAsFlux(Stmts.paramBatch(this.sql, paramGroupList, this));
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = MultiResults.fluxError(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public Publisher<RefCursor> declareCursor() {
        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            return Mono.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
        }
        this.abandonBind();
        return Mono.error(MySQLExceptions.dontSupportDeclareCursor(MySQLDriver.MY_SQL));
    }

    @Override
    public DatabaseSession abandonBind() {
        clearStatementToAvoidReuse();
        this.stmtTask.abandonBind();
        return this.session;
    }


    @Override
    public boolean isSupportPublisher() {
        // always true,ComPrepare
        return true;
    }

    @Override
    public boolean isSupportOutParameter() {
        return this.session.protocol.supportOutParameter();
    }


    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getName())
                .append("[ session : ")
                .append(this.session)
                .append(" , sql : ")
                .append(this.sql)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


    /*################################## blow packet template method ##################################*/

    @Override
    void checkReuse() throws JdbdException {
        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            throw MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        }
    }

    @Override
    void closeOnBindError(Throwable error) {
        this.stmtTask.closeOnBindError(error);
    }

    /*################################## blow private method ##################################*/


    private void clearStatementToAvoidReuse() {
        this.paramGroupList = null;
        this.paramGroup = EMPTY_PARAM_GROUP;
    }


}
