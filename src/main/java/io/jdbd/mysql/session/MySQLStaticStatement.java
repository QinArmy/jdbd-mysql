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

import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.*;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * <p>
 * This interface is a implementation of {@link StaticStatement} with MySQL protocol.
 * <br/>
 *
 * @since 1.0
 */
final class MySQLStaticStatement extends MySQLStatement<StaticStatement> implements StaticStatement {


    static MySQLStaticStatement create(final MySQLDatabaseSession<?> session) {
        return new MySQLStaticStatement(session);
    }

    private boolean executed;

    private MySQLStaticStatement(final MySQLDatabaseSession<?> session) {
        super(session);
    }


    @Override
    public Publisher<ResultStates> executeUpdate(final String sql) {
        if (this.executed) {
            return Mono.error(MySQLExceptions.cannotReuseStatement(StaticStatement.class));
        }
        this.executed = true;
        this.endStmtOption(true);
        if (!MySQLStrings.hasText(sql)) {
            return Mono.error(MySQLExceptions.sqlIsEmpty());
        }

        return this.session.protocol.update(Stmts.stmt(sql, this));
    }

    @Override
    public <M extends Publisher<ResultStates>> M executeUpdate(String sql, Function<Publisher<ResultStates>, M> monoFunc) {
        return monoFunc.apply(executeUpdate(sql));
    }

    @Override
    public Publisher<ResultRow> executeQuery(String sql) {
        return this.executeQuery(sql, CurrentRow.AS_RESULT_ROW, ResultStates.IGNORE_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function) {
        return this.executeQuery(sql, function, ResultStates.IGNORE_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(final String sql, final Function<CurrentRow, R> function,
                                         final Consumer<ResultStates> consumer) {
        if (this.executed) {
            return Mono.error(MySQLExceptions.cannotReuseStatement(StaticStatement.class));
        }
        this.executed = true;
        this.endStmtOption(true);

        if (!MySQLStrings.hasText(sql)) {
            return Flux.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.query(Stmts.stmt(sql, this), function, consumer);
    }

    @Override
    public <R, F extends Publisher<R>> F executeQuery(String sql, Function<CurrentRow, R> rowFunc,
                                                      Consumer<ResultStates> statesConsumer,
                                                      Function<Publisher<R>, F> fluxFunc) {
        return fluxFunc.apply(executeQuery(sql, rowFunc, statesConsumer));
    }


    @Override
    public Publisher<ResultStates> executeBatchUpdate(final List<String> sqlGroup) {
        if (this.executed) {
            return Flux.error(MySQLExceptions.cannotReuseStatement(StaticStatement.class));
        }
        this.executed = true;
        this.endStmtOption(true);

        if (MySQLCollections.isEmpty(sqlGroup)) {
            return Flux.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.batchUpdate(Stmts.batch(sqlGroup, this));
    }

    @Override
    public <F extends Publisher<ResultStates>> F executeBatchUpdate(List<String> sqlGroup,
                                                                    Function<Publisher<ResultStates>, F> fluxFunc) {
        return fluxFunc.apply(executeBatchUpdate(sqlGroup));
    }


    @Override
    public QueryResults executeBatchQuery(final List<String> sqlGroup) {
        if (this.executed) {
            return MultiResults.batchQueryError(MySQLExceptions.cannotReuseStatement(StaticStatement.class));
        }
        this.executed = true;
        this.endStmtOption(true);

        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.batchQueryError(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.batchQuery(Stmts.batch(sqlGroup));
    }


    @Override
    public MultiResult executeBatchAsMulti(final List<String> sqlGroup) {
        if (this.executed) {
            return MultiResults.multiError(MySQLExceptions.cannotReuseStatement(StaticStatement.class));
        }
        this.executed = true;
        this.endStmtOption(true);

        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.multiError(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.batchAsMulti(Stmts.batch(sqlGroup, this));
    }

    @Override
    public OrderedFlux executeBatchAsFlux(final List<String> sqlGroup) {
        if (this.executed) {
            return MultiResults.fluxError(MySQLExceptions.cannotReuseStatement(StaticStatement.class));
        }
        this.executed = true;
        this.endStmtOption(true);

        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.fluxError(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.batchAsFlux(Stmts.batch(sqlGroup, this));
    }

    @Override
    public <F extends Publisher<ResultItem>> F executeBatchAsFlux(List<String> sqlGroup,
                                                                  Function<OrderedFlux, F> fluxFunc) {
        return fluxFunc.apply(executeBatchAsFlux(sqlGroup));
    }


    @Override
    public OrderedFlux executeMultiStmt(final String multiStmt) {
        if (this.executed) {
            return MultiResults.fluxError(MySQLExceptions.cannotReuseStatement(StaticStatement.class));
        }
        this.executed = true;
        this.endStmtOption(true);

        final OrderedFlux flux;
        if (!this.session.protocol.supportMultiStmt()) {
            flux = MultiResults.fluxError(MySQLExceptions.dontSupportMultiStmt());
        } else if (MySQLStrings.hasText(multiStmt)) {
            flux = this.session.protocol.staticMultiStmtAsFlux(Stmts.multiStmt(multiStmt, this));
        } else {
            flux = MultiResults.fluxError(MySQLExceptions.sqlIsEmpty());
        }
        return flux;
    }

    @Override
    public <F extends Publisher<ResultItem>> F executeMultiStmt(String multiStmt, Function<OrderedFlux, F> fluxFunc) {
        return fluxFunc.apply(executeMultiStmt(multiStmt));
    }

    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getName())
                .append("[ session : ")
                .append(this.session)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


    /*################################## blow Statement packet template method ##################################*/



    /*################################## blow private static method ##################################*/


}
