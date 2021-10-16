package io.jdbd.mysql.protocol.client;


import io.jdbd.DatabaseSession;
import io.jdbd.ServerVersion;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.BindBatchStmt;
import io.jdbd.mysql.stmt.BindMultiStmt;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.StaticMultiStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import io.jdbd.vendor.task.PrepareTask;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * <p>
 * This interface is underlying api of below interfaces:
 *     <ul>
 *         <li>{@link io.jdbd.TxDatabaseSession}</li>
 *         <li>{@link io.jdbd.xa.XaDatabaseSession}</li>
 *         <li>{@link StaticStatement}</li>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 */
public interface ClientProtocol {

    long getId();


    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     *
     * @see ComQueryTask#update(StaticStmt, TaskAdjutant)
     */
    Mono<ResultStates> update(StaticStmt stmt);

    /**
     * <p>
     * This method is underlying api of below methods:
     * <ul>
     *     <li>{@link StaticStatement#executeQuery(String)}</li>
     *     <li>{@link StaticStatement#executeQuery(String, Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see ComQueryTask#query(StaticStmt, TaskAdjutant)
     */
    Flux<ResultRow> query(StaticStmt stmt);


    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatch(List)} method.
     * </p>
     *
     * @see ComQueryTask#batchUpdate(StaticBatchStmt, TaskAdjutant)
     */
    Flux<ResultStates> batchUpdate(StaticBatchStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatchAsMulti(List)} method.
     * </p>
     *
     * @see ComQueryTask#batchAsMulti(StaticBatchStmt, TaskAdjutant)
     */
    MultiResult batchAsMulti(StaticBatchStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatchAsFlux(List)} method.
     * </p>
     */
    OrderedFlux batchAsFlux(StaticBatchStmt stmt);

    OrderedFlux executeAsFlux(StaticMultiStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method.
     * </p>
     *
     * @see ComQueryTask#bindUpdate(BindStmt, TaskAdjutant)
     */
    Mono<ResultStates> bindUpdate(BindStmt wrapper);

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindStatement#executeQuery()}</li>
     *     <li>{@link BindStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see ComQueryTask#bindQuery(BindStmt, TaskAdjutant)
     */
    Flux<ResultRow> bindQuery(BindStmt wrapper);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatch()} method.
     * </p>
     *
     * @see ComQueryTask#bindBatch(BindBatchStmt, TaskAdjutant)
     */
    Flux<ResultStates> bindBatch(BindBatchStmt wrapper);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsMulti()} method.
     * </p>
     *
     * @see ComQueryTask#bindBatchAsMulti(BindBatchStmt, TaskAdjutant)
     */
    MultiResult bindBatchAsMulti(BindBatchStmt stmt);

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsFlux()} method.
     * </p>
     *
     * @see ComQueryTask#bindBatchAsFlux(BindBatchStmt, TaskAdjutant)
     */
    OrderedFlux bindBatchAsFlux(BindBatchStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatch()} method.
     * </p>
     */
    Flux<ResultStates> multiStmtBatch(BindMultiStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsMulti()} method.
     * </p>
     *
     * @see ComQueryTask#multiStmtAsMulti(BindMultiStmt, TaskAdjutant)
     */
    MultiResult multiStmtAsMulti(BindMultiStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsFlux()} method.
     * </p>
     *
     * @see ComQueryTask#multiStmtAsFlux(BindMultiStmt, TaskAdjutant)
     */
    OrderedFlux multiStmtAsFlux(BindMultiStmt stmt);

    /**
     * <p>
     * This method is underlying api of {@link DatabaseSession#prepare(String)} methods:
     * </p>
     *
     * @see ComPreparedTask#prepare(String, TaskAdjutant, Function)
     */
    Mono<PreparedStatement> prepare(String sql, Function<PrepareTask<MySQLType>, PreparedStatement> function);

    Mono<Void> reset();

    boolean supportMultiStmt();

    ServerVersion getServerVersion();

    boolean isClosed();

    Mono<Void> close();

}
