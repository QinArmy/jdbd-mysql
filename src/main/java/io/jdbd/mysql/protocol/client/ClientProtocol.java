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

package io.jdbd.mysql.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.*;
import io.jdbd.session.Option;
import io.jdbd.session.ServerVersion;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.task.PrepareTask;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
final class ClientProtocol implements MySQLProtocol {


    static ClientProtocol create(ProtocolManager manager) {
        return new ClientProtocol(manager);

    }


    final TaskAdjutant adjutant;

    private final ProtocolManager manager;

    private ClientProtocol(final ProtocolManager manager) {
        this.manager = manager;
        this.adjutant = manager.adjutant();
    }


    /*################################## blow ClientCommandProtocol method ##################################*/

    @Override
    public long sessionIdentifier() {
        return this.adjutant.handshake10().threadId;
    }

    @Override
    public Mono<ResultStates> update(StaticStmt stmt) {
        return ComQueryTask.update(stmt, this.adjutant);
    }

    @Override
    public <R> Flux<R> query(StaticStmt stmt, Function<CurrentRow, R> function, Consumer<ResultStates> consumer) {
        return ComQueryTask.query(stmt, function, consumer, this.adjutant);
    }


    @Override
    public Flux<ResultStates> batchUpdate(StaticBatchStmt stmt) {
        return ComQueryTask.batchUpdate(stmt, this.adjutant);
    }

    @Override
    public QueryResults batchQuery(StaticBatchStmt stmt) {
        return ComQueryTask.batchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult batchAsMulti(final StaticBatchStmt stmt) {
        return ComQueryTask.batchAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux batchAsFlux(final StaticBatchStmt stmt) {
        return ComQueryTask.batchAsFlux(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux staticMultiStmtAsFlux(StaticMultiStmt stmt) {
        return ComQueryTask.staticMultiStmt(stmt, this.adjutant);
    }

    @Override
    public Mono<ResultStates> paramUpdate(ParamStmt stmt, boolean usePrepare) {
        final Mono<ResultStates> mono;
        if (usePrepare) {
            mono = ComPreparedTask.update(stmt, this.adjutant);
        } else {
            mono = ComQueryTask.paramUpdate(stmt, this.adjutant);
        }
        return mono;
    }

    @Override
    public <R> Flux<R> paramQuery(ParamStmt stmt, boolean usePrepare, Function<CurrentRow, R> function,
                                  Consumer<ResultStates> consumer) {
        final Flux<R> flux;
        if (usePrepare) {
            flux = ComPreparedTask.query(stmt, function, consumer, this.adjutant);
        } else {
            flux = ComQueryTask.paramQuery(stmt, function, consumer, this.adjutant);
        }
        return flux;
    }

    @Override
    public OrderedFlux paramAsFlux(ParamStmt stmt, boolean usePrepare) {
        final OrderedFlux flux;
        if (usePrepare) {
            flux = ComPreparedTask.asFlux(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.paramAsFlux(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public Flux<ResultStates> paramBatchUpdate(ParamBatchStmt stmt, boolean usePrepare) {
        final Flux<ResultStates> flux;
        if (usePrepare) {
            flux = ComPreparedTask.batchUpdate(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.paramBatchUpdate(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public <R> Flux<R> paramBatchQueryAsFlux(ParamBatchStmt stmt, boolean usePrepare, Function<CurrentRow, R> function,
                                             Consumer<ResultStates> consumer) {
        final Flux<R> flux;
        if (usePrepare) {
            flux = ComPreparedTask.batchQueryAsFlux(stmt, function, consumer, this.adjutant);
        } else {
            flux = ComQueryTask.paramBatchQueryAsFlux(stmt, function, consumer, this.adjutant);
        }
        return flux;
    }

    @Override
    public QueryResults paramBatchQuery(ParamBatchStmt stmt, boolean usePrepare) {
        final QueryResults batchQuery;
        if (usePrepare) {
            batchQuery = ComPreparedTask.batchQuery(stmt, this.adjutant);
        } else {
            batchQuery = ComQueryTask.paramBatchQuery(stmt, this.adjutant);
        }
        return batchQuery;
    }

    @Override
    public MultiResult paramBatchAsMulti(final ParamBatchStmt stmt, final boolean usePrepare) {
        final MultiResult result;
        if (usePrepare) {
            result = ComPreparedTask.batchAsMulti(stmt, this.adjutant);
        } else {
            result = ComQueryTask.paramBatchAsMulti(stmt, this.adjutant);
        }
        return result;
    }

    @Override
    public OrderedFlux paramBatchAsFlux(final ParamBatchStmt stmt, final boolean usePrepare) {
        final OrderedFlux flux;
        if (usePrepare) {
            flux = ComPreparedTask.batchAsFlux(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.paramBatchAsFlux(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public Flux<ResultStates> multiStmtBatchUpdate(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtBatchUpdate(stmt, this.adjutant);
    }

    @Override
    public QueryResults multiStmtBatchQuery(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtBatchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult multiStmtAsMulti(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux multiStmtAsFlux(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtAsFlux(stmt, this.adjutant);
    }

    @Override
    public Mono<PrepareTask> prepare(String sql) {
        return ComPreparedTask.prepare(sql, this.adjutant);
    }

    @Override
    public boolean supportOutParameter() {
        return this.adjutant.handshake10().serverVersion.isSupportOutParameter();
    }

    @Override
    public boolean supportStmtVar() {
        return this.adjutant.handshake10().serverVersion.isSupportQueryAttr();
    }


    @Override
    public boolean inTransaction() {
        return Terminator.inTransaction(this.adjutant.serverStatus());
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T> T valueOf(final @Nullable Option<T> option) {
        final TaskAdjutant adjutant = this.adjutant;
        if (!adjutant.isActive()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
        final int serverStatus = adjutant.serverStatus();
        final Object value;
        if (option == null) {
            value = null;
        } else if (option == Option.AUTO_COMMIT) {
            value = (serverStatus & Terminator.SERVER_STATUS_AUTOCOMMIT) != 0;
        } else if (option == Option.IN_TRANSACTION) {
            value = Terminator.inTransaction(serverStatus);
        } else if (option == Option.READ_ONLY) {
            value = (serverStatus & Terminator.SERVER_STATUS_IN_TRANS_READONLY) != 0;
        } else if (option == Option.BACKSLASH_ESCAPES) {
            value = (serverStatus & Terminator.SERVER_STATUS_NO_BACKSLASH_ESCAPES) == 0;
        } else if (option == Option.BINARY_HEX_ESCAPES) {
            value = Boolean.TRUE;
        } else if (option == Option.CLIENT_ZONE) {
            value = adjutant.sessionEnv().connZone();
        } else if (option == Option.SERVER_ZONE) {
            value = adjutant.sessionEnv().serverZone();
        } else if (option == Option.CLIENT_CHARSET) {
            value = adjutant.sessionEnv().charsetClient();
        } else {
            value = null;
        }
        return (T) value;
    }

    @Override
    public Set<Option<?>> optionSet() {
        return OptionSetHolder.OPTION_SET;
    }


    @Override
    public void addSessionCloseListener(Runnable listener) {
        this.adjutant.addSessionCloseListener(listener);
    }

    @Override
    public void addTransactionEndListener(Runnable listener) {
        this.adjutant.addTransactionEndListener(listener);
    }

    @Override
    public <T> Mono<T> close() {
        // io.jdbd.session.DatabaseSession is responsible for parallel.
        return Mono.defer(this::closeProtocol);
    }

    @Override
    public boolean isClosed() {
        // io.jdbd.session.DatabaseSession is responsible for parallel.
        return !this.adjutant.isActive();
    }


    @Override
    public Mono<Void> reset() {
        return Mono.defer(this.manager::reset);
    }

    @Override
    public Mono<Void> ping() {
        return PingTask.ping(this.adjutant);
    }

    @Override
    public Mono<Void> softClose() {
        return this.adjutant.softClose();
    }

    @Override
    public boolean supportMultiStmt() {
        return Capabilities.supportMultiStatement(this.adjutant.capability());
    }

    @Override
    public ServerVersion serverVersion() {
        return this.adjutant.handshake10().serverVersion;
    }



    /*################################## blow private method ##################################*/


    /**
     * @see #close()
     */
    private <T> Mono<T> closeProtocol() {
        // io.jdbd.session.DatabaseSession is responsible for parallel.
        final Mono<T> mono;
        if (this.adjutant.isActive()) {
            mono = QuitTask.quit(this.adjutant);
        } else {
            mono = Mono.empty();
        }
        return mono;
    }


    private static Set<Option<?>> sessionOptionSet() {
        final Set<Option<?>> set = MySQLCollections.hashSet();

        set.add(Option.AUTO_COMMIT);
        set.add(Option.IN_TRANSACTION);
        set.add(Option.READ_ONLY);

        set.add(Option.BACKSLASH_ESCAPES);
        set.add(Option.BINARY_HEX_ESCAPES);
        set.add(Option.CLIENT_ZONE);
        set.add(Option.SERVER_ZONE);

        set.add(Option.CLIENT_CHARSET);

        return MySQLCollections.unmodifiableSet(set);
    }

    private static abstract class OptionSetHolder {
        private static final Set<Option<?>> OPTION_SET = sessionOptionSet();

    } // OptionSetHolder


}
