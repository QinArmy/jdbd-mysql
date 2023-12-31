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
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.type.Blob;
import io.jdbd.type.Clob;
import io.jdbd.vendor.ResultType;
import io.jdbd.vendor.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamBatchStmt;
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
 * <br/>
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
        } else if ((type = MySQLBinds.mapDataType(dataType)) == MySQLType.UNKNOWN) {
            error = MySQLExceptions.dontSupportDataType(dataType, MY_SQL);
        } else {
            error = null;
            if (paramGroup == null) {
                this.paramGroup = paramGroup = MySQLCollections.arrayList(paramCount);
            }
            final Object actualValue;
            if (value instanceof String && ((String) value).length() > 0xff_ff_ff) {
                actualValue = Clob.from(Mono.just((String) value));
            } else if (value instanceof byte[] && ((byte[]) value).length > 0xff_ff_ff) {
                actualValue = Blob.from(Mono.just(((byte[]) value)));
            } else {
                actualValue = value;
            }
            paramGroup.add(JdbdValues.paramValue(indexBasedZero, type, actualValue));
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
        this.endStmtOption(true);

        List<ParamValue> paramGroup = this.paramGroup;
        final int paramSize = paramGroup == null ? 0 : paramGroup.size();

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta.getColumnCount() > 0) {
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
            mono = this.stmtTask.executeUpdate(Stmts.paramStmt(this.sql, paramGroup, this));
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            mono = Mono.error(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return mono;
    }

    @Override
    public <R extends Publisher<ResultStates>> R executeUpdate(Function<Publisher<ResultStates>, R> monoFunc) {
        return monoFunc.apply(executeUpdate());
    }

    @Override
    public Publisher<ResultRow> executeQuery() {
        return this.executeQuery(CurrentRow.AS_RESULT_ROW, ResultStates.IGNORE_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(Function<CurrentRow, R> function) {
        return this.executeQuery(function, ResultStates.IGNORE_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(final @Nullable Function<CurrentRow, R> function,
                                         final @Nullable Consumer<ResultStates> consumer) {
        this.endStmtOption(false);

        List<ParamValue> paramGroup = this.paramGroup;
        final int paramSize = paramGroup == null ? 0 : paramGroup.size();

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta.getColumnCount() == 0) {
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
            flux = this.stmtTask.executeQuery(Stmts.paramStmt(this.sql, paramGroup, this), function, consumer);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public <R, F extends Publisher<R>> F executeQuery(Function<CurrentRow, R> rowFunc,
                                                      Consumer<ResultStates> statesConsumer,
                                                      @Nullable Function<Publisher<R>, F> fluxFunc) {
        if (fluxFunc == null) {
            final NullPointerException error;
            error = MySQLExceptions.fluxFuncIsNull();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }
        return fluxFunc.apply(executeQuery(rowFunc, statesConsumer));
    }

    @Override
    public OrderedFlux executeAsFlux() {
        this.endStmtOption(false);

        List<ParamValue> paramGroup = this.paramGroup;
        final int paramSize = paramGroup == null ? 0 : paramGroup.size();

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.paramGroupList != null) {
            error = new SubscribeException(ResultType.FLUX, ResultType.BATCH);
        } else if (paramSize != this.paramCount) {
            error = MySQLExceptions.parameterCountMatch(0, this.paramCount, paramSize);
        } else if (paramGroup == null) {
            error = null;
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final OrderedFlux flux;
        if (error == null) {
            flux = this.stmtTask.executeAsFlux(Stmts.paramStmt(this.sql, paramGroup, this));
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = MultiResults.fluxError(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public <F extends Publisher<ResultItem>> F executeAsFlux(@Nullable Function<OrderedFlux, F> fluxFunc) {
        if (fluxFunc == null) {
            final NullPointerException error;
            error = MySQLExceptions.fluxFuncIsNull();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }
        return fluxFunc.apply(executeAsFlux());
    }

    @Override
    public Publisher<ResultStates> executeBatchUpdate() {
        this.endStmtOption(true);

        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else if (this.rowMeta.getColumnCount() > 0) {
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
    public <R> Publisher<R> executeBatchQueryAsFlux(Function<CurrentRow, R> rowFunc, Consumer<ResultStates> statesConsumer) {
        this.endStmtOption(true);

        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else if (this.rowMeta.getColumnCount() == 0) {
            error = new SubscribeException(ResultType.BATCH_QUERY, ResultType.UPDATE);
        } else {
            error = null;
        }

        final Flux<R> flux;
        if (error == null) {
            final ParamBatchStmt stmt;
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            flux = this.stmtTask.executeBatchQueryAsFlux(stmt, rowFunc, statesConsumer);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public QueryResults executeBatchQuery() {
        this.endStmtOption(true);

        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else if (this.rowMeta.getColumnCount() == 0) {
            error = new SubscribeException(ResultType.BATCH_QUERY, ResultType.UPDATE);
        } else {
            error = null;
        }

        final QueryResults batchQuery;
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
    public MultiResult executeBatchAsMulti() {
        this.endStmtOption(true);

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
        this.endStmtOption(true);

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
    public DatabaseSession abandonBind() {
        clearStatementToAvoidReuse();
        this.stmtTask.abandonBind();
        return this.session;
    }

    @Override
    public PreparedStatement setFrequency(final int frequency) throws IllegalArgumentException {
        if (frequency < 0) {
            throw MySQLExceptions.frequencyIsNegative(frequency);
        }
        this.frequency = frequency;
        return this;
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
    void closeOnBindError(Throwable error) {
        this.stmtTask.closeOnBindError(error);
    }

    /*################################## blow private method ##################################*/


    private void clearStatementToAvoidReuse() {
        this.paramGroupList = null;
        this.paramGroup = EMPTY_PARAM_GROUP;
    }


}
