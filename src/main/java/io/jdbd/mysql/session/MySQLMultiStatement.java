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
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.QueryResults;
import io.jdbd.result.ResultStates;
import io.jdbd.statement.MultiStatement;
import io.jdbd.statement.Parameter;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.JdbdBinds;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.List;

import static io.jdbd.mysql.MySQLDriver.MY_SQL;

/**
 * <p>
 * This interface is a implementation of {@link MultiStatement} with MySQL client protocol.
 * <br/>
 *
 * @since 1.0
 */
final class MySQLMultiStatement extends MySQLStatement<MultiStatement> implements MultiStatement {

    static MySQLMultiStatement create(MySQLDatabaseSession<?> session) {
        return new MySQLMultiStatement(session);
    }

    private MySQLMultiStatement(MySQLDatabaseSession<?> session) {
        super(session);
    }

    private List<ParamStmt> stmtGroup = MySQLCollections.arrayList();

    private String currentSql;

    /**
     * current bind group.
     */
    private List<ParamValue> paramGroup = null;


    @Override
    public MultiStatement addStatement(final String sql) throws JdbdException {

        final List<ParamValue> paramGroup = this.paramGroup;
        final RuntimeException error;

        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(MultiStatement.class);
        } else if (!MySQLStrings.hasText(sql)) {
            error = MySQLExceptions.sqlIsEmpty();
        } else if (paramGroup == null) {
            error = null;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(this.stmtGroup.size(), paramGroup);
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw MySQLExceptions.wrap(error);
        }

        final String lastSql = this.currentSql;
        if (lastSql != null) {
            this.stmtGroup.add(Stmts.paramStmt(lastSql, paramGroup, this));
        }

        this.currentSql = sql;
        this.paramGroup = null;
        return this;
    }


    @Override
    public MultiStatement bind(final int indexBasedZero, final @Nullable DataType dataType,
                               final @Nullable Object value) throws JdbdException {

        List<ParamValue> paramGroup = this.paramGroup;

        final MySQLType type;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(MultiStatement.class);
        } else if (indexBasedZero < 0) {
            error = MySQLExceptions.invalidParameterValue(this.stmtGroup.size(), indexBasedZero);
        } else if (value instanceof Parameter) {
            error = MySQLExceptions.dontSupportJavaType(indexBasedZero, value, MY_SQL);
        } else if (dataType == null) {
            error = MySQLExceptions.dataTypeIsNull();
        } else if (value != null && (dataType == JdbdType.NULL || dataType == MySQLType.NULL)) {
            error = MySQLExceptions.nonNullBindValueOf(dataType);
        } else if ((type = MySQLBinds.mapDataType(dataType)) == MySQLType.UNKNOWN) {
            error = MySQLExceptions.dontSupportDataType(dataType, MY_SQL);
        } else {
            error = null;
            if (paramGroup == null) {
                this.paramGroup = paramGroup = MySQLCollections.arrayList();
            }
            paramGroup.add(JdbdValues.paramValue(indexBasedZero, type, value));
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw MySQLExceptions.wrap(error);
        }
        return this;
    }


    @Override
    public Publisher<ResultStates> executeBatchUpdate() {

        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            return Flux.error(MySQLExceptions.cannotReuseStatement(MultiStatement.class));
        }
        this.endMultiStatement();

        final List<ParamStmt> stmtGroup = this.stmtGroup;
        final Flux<ResultStates> flux;
        if (stmtGroup.size() == 0) {
            flux = Flux.error(MySQLExceptions.multiStmtNoSql());
        } else {
            flux = this.session.protocol.multiStmtBatchUpdate(Stmts.paramMultiStmt(stmtGroup, this));
        }
        clearStatementToAvoidReuse();
        return flux;
    }


    @Override
    public QueryResults executeBatchQuery() {
        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            return MultiResults.batchQueryError(MySQLExceptions.cannotReuseStatement(MultiStatement.class));
        }

        this.endMultiStatement();

        final List<ParamStmt> stmtGroup = this.stmtGroup;
        final QueryResults batchQuery;
        if (stmtGroup.size() == 0) {
            batchQuery = MultiResults.batchQueryError(MySQLExceptions.multiStmtNoSql());
        } else {
            batchQuery = this.session.protocol.multiStmtBatchQuery(Stmts.paramMultiStmt(stmtGroup, this));
        }
        clearStatementToAvoidReuse();
        return batchQuery;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            return MultiResults.multiError(MySQLExceptions.cannotReuseStatement(MultiStatement.class));
        }

        this.endMultiStatement();

        final List<ParamStmt> stmtGroup = this.stmtGroup;
        final MultiResult multiResult;
        if (stmtGroup.size() == 0) {
            multiResult = MultiResults.multiError(MySQLExceptions.multiStmtNoSql());
        } else {
            multiResult = this.session.protocol.multiStmtAsMulti(Stmts.paramMultiStmt(stmtGroup, this));
        }
        clearStatementToAvoidReuse();
        return multiResult;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {

        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            return MultiResults.fluxError(MySQLExceptions.cannotReuseStatement(MultiStatement.class));
        }

        this.endMultiStatement();

        final List<ParamStmt> stmtGroup = this.stmtGroup;
        final OrderedFlux flux;
        if (stmtGroup.size() == 0) {
            flux = MultiResults.fluxError(MySQLExceptions.multiStmtNoSql());
        } else {
            flux = this.session.protocol.multiStmtAsFlux(Stmts.paramMultiStmt(stmtGroup, this));
        }
        clearStatementToAvoidReuse();
        return flux;
    }


    /*################################## blow Statement method ##################################*/

    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getName())
                .append("[ session : ")
                .append(this.session)
                .append(" , sqlList size : ")
                .append(this.stmtGroup.size())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }

    /*################################## blow MySQLStatement packet template method ##################################*/


    /*################################## blow private method ##################################*/

    private void clearStatementToAvoidReuse() {
        this.currentSql = null;
        this.paramGroup = EMPTY_PARAM_GROUP;
        this.stmtGroup = null;
    }

    private void endMultiStatement() {
        this.endStmtOption(true);

        final String lastSql = this.currentSql;
        if (lastSql != null) {
            this.stmtGroup.add(Stmts.paramStmt(lastSql, this.paramGroup, this));
        }
        this.currentSql = null;
        this.paramGroup = null;
    }


}
