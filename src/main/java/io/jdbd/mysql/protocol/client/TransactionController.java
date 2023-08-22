package io.jdbd.mysql.protocol.client;

import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * @see ClientProtocol
 */
interface TransactionController {

    Mono<ResultStates> startTransaction(TransactionOption option, HandleMode mode);

    Mono<TransactionStatus> transactionStatus();
    Mono<ResultStates> start(Xid xid, int flags, TransactionOption option);

    Mono<ResultStates> end(Xid xid, int flags, Function<Option<?>, ?> optionFunc);

    Mono<Integer> prepare(Xid xid, Function<Option<?>, ?> optionFunc);

    Mono<ResultStates> commit(Xid xid,  int flags, Function<Option<?>, ?> optionFunc);

    Mono<RmDatabaseSession> rollback(Xid xid, Function<Option<?>, ?> optionFunc);

}
