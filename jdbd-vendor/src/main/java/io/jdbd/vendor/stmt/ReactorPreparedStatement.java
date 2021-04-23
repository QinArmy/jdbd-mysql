package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;


/**
 * <p>
 * This interface override return type below methods to avoid type error:
 *     <ul>
 *         <li>{@link #executeBatch()}</li>
 *         <li>{@link #executeUpdate()}</li>
 *         <li>{@link #executeQuery()}</li>
 *         <li>{@link #executeQuery(Consumer)}</li>
 *         <li>{@link #executeAsMulti()}</li>
 *         <li>{@link #executeAsFlux()}</li>
 *     </ul>
 * </p>
 */
public interface ReactorPreparedStatement extends PreparedStatement {

    @Override
    Flux<ResultStatus> executeBatch();

    @Override
    Mono<ResultStatus> executeUpdate();

    @Override
    Flux<ResultRow> executeQuery();

    @Override
    Flux<ResultRow> executeQuery(Consumer<ResultStatus> statesConsumer);

    @Override
    ReactorMultiResult executeAsMulti();

    @Override
    Flux<SingleResult> executeAsFlux();


}