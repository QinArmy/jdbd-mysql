package io.jdbd.mysql.protocol;

import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * <p>
 * This interface provide the ability that use postgre protocol for MySQL {@link DatabaseSession}.
 * </p>
 *
 * @since 1.0
 */
public interface MySQLProtocol extends DatabaseProtocol {


    Mono<ResultStates> start(Xid xid, int flags, TransactionOption option);

    Mono<ResultStates> end(Xid xid, int flags, Function<Option<?>, ?> optionFunc);

    Mono<Integer> prepare(Xid xid, Function<Option<?>, ?> optionFunc);

    Mono<ResultStates> commit(Xid xid,  int flags, Function<Option<?>, ?> optionFunc);

    Mono<RmDatabaseSession> rollback(Xid xid, Function<Option<?>, ?> optionFunc);

}
