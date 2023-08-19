package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.pool.PoolLocalDatabaseSession;
import io.jdbd.result.ResultStates;
import io.jdbd.session.HandleMode;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.session.TransactionOption;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;

/**
 * <p>
 * This class is implementation of {@link LocalDatabaseSession} with MySQL client protocol.
 * </p>
 */
class MySQLLocalDatabaseSession extends MySQLDatabaseSession<LocalDatabaseSession> implements LocalDatabaseSession {


    static LocalDatabaseSession create(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        return new MySQLLocalDatabaseSession(factory, protocol);
    }

    static PoolLocalDatabaseSession forPoolVendor(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        return new MySQLPoolLocalDatabaseSession(factory, protocol);
    }

    /**
     * private constructor
     */
    private MySQLLocalDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        super(factory, protocol);
    }


    @Override
    public final Publisher<LocalDatabaseSession> startTransaction(TransactionOption option) {
        return this.startTransaction(option, HandleMode.ERROR_IF_EXISTS);
    }

    @Override
    public Publisher<LocalDatabaseSession> startTransaction(final TransactionOption option, final HandleMode mode) {
        return this.protocol.startTransaction(option, mode)
                .flatMap(this::afterStartTransaction);
    }

    @Override
    public final boolean inTransaction() {
        return this.protocol.inTransaction();
    }

    @Override
    public final Publisher<LocalDatabaseSession> commit() {
        return this.commit(Collections.emptyList());
    }

    @Override
    public final Mono<LocalDatabaseSession> commit(List<Option<?>> optionList) {
        return this.protocol.commit(optionList)
                .thenReturn(this);
    }

    @Override
    public final Publisher<LocalDatabaseSession> rollback() {
        return this.rollback(Collections.emptyList());
    }

    @Override
    public final Mono<LocalDatabaseSession> rollback(List<Option<?>> optionList) {
        return this.protocol.rollback(optionList)
                .thenReturn(this);
    }


    private Mono<LocalDatabaseSession> afterStartTransaction(ResultStates states) {
        if (states.inTransaction()) {
            return Mono.just(this);
        }
        return Mono.error(MySQLExceptions.startTransactionFailure(this.protocol.threadId()));
    }


    /**
     * <p>
     * This class is implementation of {@link PoolLocalDatabaseSession} with MySQL client protocol.
     * </p>
     *
     * @since 1.0
     */
    private static final class MySQLPoolLocalDatabaseSession extends MySQLLocalDatabaseSession
            implements PoolLocalDatabaseSession {

        private MySQLPoolLocalDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
            super(factory, protocol);
        }


        @Override
        public Publisher<PoolLocalDatabaseSession> reconnect() {
            return this.protocol.reconnect()
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolLocalDatabaseSession> ping(int timeoutSeconds) {
            return this.protocol.ping(timeoutSeconds)
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolLocalDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }


    }// MySQLPoolLocalDatabaseSession


}
