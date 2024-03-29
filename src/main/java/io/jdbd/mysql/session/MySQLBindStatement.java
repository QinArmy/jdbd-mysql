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
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.Parameter;
import io.jdbd.type.Blob;
import io.jdbd.type.Clob;
import io.jdbd.vendor.ResultType;
import io.jdbd.vendor.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.*;
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
 * This interface is a implementation of {@link BindStatement} with MySQL client protocol.
 * <br/>
 */
final class MySQLBindStatement extends MySQLStatement<BindStatement> implements BindStatement {

    static MySQLBindStatement create(MySQLDatabaseSession<?> session, String sql, boolean forcePrepare) {
        return new MySQLBindStatement(session, sql, forcePrepare);
    }

    private final String sql;

    private final boolean forcePrepare;

    private List<List<ParamValue>> paramGroupList;

    private List<ParamValue> paramGroup;

    private int firstParamSize = -1;

    private boolean usePrepare;

    public MySQLBindStatement(final MySQLDatabaseSession<?> session, final String sql, final boolean forcePrepare) {
        super(session);
        this.sql = sql;
        this.forcePrepare = forcePrepare;
        this.usePrepare = forcePrepare;
    }

    @Override
    public boolean isForcePrepare() {
        return this.forcePrepare;
    }


    @Override
    public BindStatement bind(final int indexBasedZero, final @Nullable DataType dataType,
                              final @Nullable Object value) throws JdbdException {

        List<ParamValue> paramGroup = this.paramGroup;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            throw MySQLExceptions.cannotReuseStatement(BindStatement.class);
        }

        final int firstGroupSize = this.firstParamSize;

        final RuntimeException error;
        final MySQLType type;

        if (indexBasedZero < 0) {
            List<List<ParamValue>> paramGroupList = this.paramGroupList;
            final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();
            error = MySQLExceptions.invalidParameterValue(groupSize, indexBasedZero);
        } else if (indexBasedZero == firstGroupSize) {
            List<List<ParamValue>> paramGroupList = this.paramGroupList;
            final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();
            error = MySQLExceptions.notMatchWithFirstParamGroupCount(groupSize, indexBasedZero, firstGroupSize);
        } else if (dataType == null) {
            error = MySQLExceptions.dataTypeIsNull();
        } else if (value != null && (dataType == JdbdType.NULL || dataType == MySQLType.NULL)) {
            error = MySQLExceptions.nonNullBindValueOf(dataType);
        } else if ((type = MySQLBinds.mapDataType(dataType)) == MySQLType.UNKNOWN) {
            error = MySQLExceptions.dontSupportDataType(dataType, MY_SQL);
        } else {
            error = null;
            if (paramGroup == null) {
                this.paramGroup = paramGroup = MySQLCollections.arrayList(firstGroupSize < 0 ? 0 : firstGroupSize);
            }
            final Object actualValue;
            if (value instanceof Parameter) { // TODO long string or binary or out parameter
                this.usePrepare = true;
                actualValue = value;
            } else if (value instanceof String && ((String) value).length() > 0xff_ff_ff) {
                this.usePrepare = true;
                actualValue = Clob.from(Mono.just((String) value));
            } else if (value instanceof byte[] && ((byte[]) value).length > 0xff_ff_ff) {
                this.usePrepare = true;
                actualValue = Blob.from(Mono.just(((byte[]) value)));
            } else {
                actualValue = value;
            }
            paramGroup.add(JdbdValues.paramValue(indexBasedZero, type, actualValue));
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw MySQLExceptions.wrap(error);
        }
        return this;
    }


    @Override
    public BindStatement addBatch() throws JdbdException {

        // add bind group
        final List<ParamValue> paramGroup = this.paramGroup;

        if (paramGroup == EMPTY_PARAM_GROUP) {
            throw MySQLExceptions.cannotReuseStatement(BindStatement.class);
        }
        List<List<ParamValue>> paramGroupList = this.paramGroupList;

        final int paramSize = paramGroup == null ? 0 : paramGroup.size();
        final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();

        int firstParamSize = this.firstParamSize;
        if (firstParamSize < 0) {
            this.firstParamSize = firstParamSize = paramSize;
        }

        final RuntimeException error;
        if (paramSize != firstParamSize) {
            error = MySQLExceptions.notMatchWithFirstParamGroupCount(groupSize, paramSize, firstParamSize);
        } else if (paramGroup == null) {
            error = null;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(groupSize, paramGroup);
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw MySQLExceptions.wrap(error);
        }

        if (paramGroupList == null) {
            this.paramGroupList = paramGroupList = MySQLCollections.arrayList();
        }

        if (paramGroup == null) {
            paramGroupList.add(EMPTY_PARAM_GROUP);
        } else {
            paramGroupList.add(MySQLCollections.unmodifiableList(paramGroup));
        }
        this.paramGroup = null; // clear for next batch
        return this;
    }


    @Override
    public Mono<ResultStates> executeUpdate() {
        this.endStmtOption(true);

        final List<ParamValue> paramGroup = this.paramGroup;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            throw MySQLExceptions.cannotReuseStatement(BindStatement.class);
        }

        final RuntimeException error;
        if (this.paramGroupList != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH_UPDATE);
        } else if (paramGroup == null) {
            error = null;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final Mono<ResultStates> mono;
        if (error != null) {
            mono = Mono.error(MySQLExceptions.wrap(error));
        } else if (!this.forcePrepare && (paramGroup == null || paramGroup.size() == 0)) {
            mono = this.session.protocol.update(Stmts.stmt(this.sql, this));
        } else {
            mono = this.session.protocol.paramUpdate(Stmts.paramStmt(this.sql, paramGroup, this), isUsePrepare());
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
    public <R> Publisher<R> executeQuery(final Function<CurrentRow, R> function, final Consumer<ResultStates> consumer) {
        this.endStmtOption(false);

        final List<ParamValue> paramGroup = this.paramGroup;
        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (this.paramGroupList != null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH_UPDATE);
        } else if (paramGroup == null) {
            error = null;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }
        final Flux<R> flux;
        if (error == null) {
            final ParamStmt stmt;
            stmt = Stmts.paramStmt(this.sql, paramGroup, this);
            flux = this.session.protocol.paramQuery(stmt, isUsePrepare(), function, consumer);
        } else {
            flux = Flux.error(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public <R, F extends Publisher<R>> F executeQuery(Function<CurrentRow, R> rowFunc,
                                                      Consumer<ResultStates> statesConsumer,
                                                      Function<Publisher<R>, F> fluxFunc) {
        return fluxFunc.apply(executeQuery(rowFunc, statesConsumer));
    }

    @Override
    public OrderedFlux executeAsFlux() {
        this.endStmtOption(false);

        final List<ParamValue> paramGroup = this.paramGroup;
        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (this.paramGroupList != null) {
            error = new SubscribeException(ResultType.MULTI_RESULT, ResultType.BATCH_UPDATE);
        } else if (paramGroup == null) {
            error = null;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }
        final OrderedFlux flux;
        if (error == null) {
            final ParamStmt stmt;
            stmt = Stmts.paramStmt(this.sql, paramGroup, this);
            flux = this.session.protocol.paramAsFlux(stmt, isUsePrepare());
        } else {
            flux = MultiResults.fluxError(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public <F extends Publisher<ResultItem>> F executeAsFlux(Function<OrderedFlux, F> fluxFunc) {
        return fluxFunc.apply(executeAsFlux());
    }

    @Override
    public Publisher<ResultStates> executeBatchUpdate() {
        this.endStmtOption(true);

        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }
        final Flux<ResultStates> flux;
        if (error == null) {
            final ParamBatchStmt stmt;
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            flux = this.session.protocol.paramBatchUpdate(stmt, isUsePrepare());
        } else {
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
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }
        final Flux<R> flux;
        if (error == null) {
            final ParamBatchStmt stmt;
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            flux = this.session.protocol.paramBatchQueryAsFlux(stmt, isUsePrepare(), rowFunc, statesConsumer);
        } else {
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
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }
        final QueryResults batchQuery;
        if (error == null) {
            final ParamBatchStmt stmt;
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            batchQuery = this.session.protocol.paramBatchQuery(stmt, isUsePrepare());
        } else {
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
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }
        final MultiResult multiResult;
        if (error == null) {
            final ParamBatchStmt stmt;
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            multiResult = this.session.protocol.paramBatchAsMulti(stmt, isUsePrepare());
        } else {
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
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (paramGroup != null) {
            error = MySQLExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null) {
            error = MySQLExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }
        final OrderedFlux flux;
        if (error == null) {
            final ParamBatchStmt stmt;
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            flux = this.session.protocol.paramBatchAsFlux(stmt, isUsePrepare());
        } else {
            flux = MultiResults.fluxError(MySQLExceptions.wrap(error));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public BindStatement setFrequency(final int frequency) throws IllegalArgumentException {
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
                .append(" , forcePrepare : ")
                .append(this.forcePrepare)
                .append(" , usePrepare : ")
                .append(this.usePrepare)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


    /*################################## blow packet template method ##################################*/


    /*################################## blow private method ##################################*/


    private void clearStatementToAvoidReuse() {
        this.paramGroupList = null;
        this.paramGroup = EMPTY_PARAM_GROUP;
    }

    private boolean isUsePrepare() {
        return this.forcePrepare || this.usePrepare || this.fetchSize > 0;
    }


}
