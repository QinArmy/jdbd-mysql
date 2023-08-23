package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.pool.PoolLocalDatabaseSession;
import io.jdbd.result.ResultItem;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

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


    private static final AtomicReferenceFieldUpdater<MySQLLocalDatabaseSession, CurrentTxOption> CURRENT_TX_OPTION =
            AtomicReferenceFieldUpdater.newUpdater(MySQLLocalDatabaseSession.class, CurrentTxOption.class, "currentTxOption");


    private volatile CurrentTxOption currentTxOption;


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
    public Publisher<LocalDatabaseSession> startTransaction(final @Nullable TransactionOption option,
                                                            final @Nullable HandleMode mode) {
        if (option == null || mode == null) {
            return Mono.error(new NullPointerException());
        }

        final StringBuilder builder = new StringBuilder(50);
        final CurrentTxOption currentTxOption = this.currentTxOption;
        final JdbdException error;
        if ((currentTxOption != null || this.protocol.inTransaction())
                && (error = handleInTransaction(mode, builder)) != null) {
            return Mono.error(error);
        }

        final Isolation isolation;
        isolation = option.isolation();

        if (isolation == null) {
            builder.append("SET @@transaction_isolation =  @@SESSION.transaction_isolation ; ") // here,must guarantee isolation is session isolation
                    .append("SELECT @@SESSION.transaction_isolation AS txIsolationLevel ; ");
        } else {
            builder.append("SET TRANSACTION ISOLATION LEVEL ");
            if (MySQLDatabaseSession.appendIsolation(isolation, builder)) {
                return Mono.error(MySQLExceptions.unknownIsolation(isolation));
            }
            builder.append(Constants.SPACE_SEMICOLON_SPACE);
        }

        builder.append("START TRANSACTION ");
        if (option.isReadOnly()) {
            builder.append("READ ONLY");
        } else {
            builder.append("READ WRITE");
        }

        final Boolean consistentSnapshot;
        consistentSnapshot = option.valueOf(Option.WITH_CONSISTENT_SNAPSHOT);


        if (Boolean.TRUE.equals(consistentSnapshot)) {
            builder.append(Constants.SPACE_COMMA_SPACE)
                    .append("WITH CONSISTENT SNAPSHOT");
        }

        final AtomicReference<Isolation> isolationHolder = new AtomicReference<>(isolation);

        return Flux.from(this.protocol.executeAsFlux(Stmts.multiStmt(builder.toString())))
                .doOnNext(item -> handleStartTransactionResult(item, isolationHolder, consistentSnapshot))
                .doOnError(e -> CURRENT_TX_OPTION.set(this, null))
                .then(Mono.just(this));
    }


    @Override
    public final Publisher<LocalDatabaseSession> commit() {
        return this.commit(DatabaseProtocol.OPTION_FUNC);
    }

    @Override
    public final Mono<LocalDatabaseSession> commit(Function<Option<?>, ?> optionFunc) {
        return this.protocol.commit(optionFunc)
                .thenReturn(this);
    }

    @Override
    public final Publisher<LocalDatabaseSession> rollback() {
        return this.rollback(DatabaseProtocol.OPTION_FUNC);
    }

    @Override
    public final Mono<LocalDatabaseSession> rollback(Function<Option<?>, ?> optionFunc) {
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
     * @see #startTransaction(TransactionOption, HandleMode)
     */
    @Nullable
    private JdbdException handleInTransaction(final HandleMode mode, final StringBuilder builder) {
        JdbdException error = null;
        switch (mode) {
            case ERROR_IF_EXISTS:
                error = MySQLExceptions.transactionExistsRejectStart(this.protocol.sessionIdentifier());
                break;
            case COMMIT_IF_EXISTS:
                builder.append(COMMIT)
                        .append(Constants.SPACE_SEMICOLON_SPACE);
                break;
            case ROLLBACK_IF_EXISTS:
                builder.append(ROLLBACK)
                        .append(Constants.SPACE_SEMICOLON_SPACE);
                break;
            default:
                error = MySQLExceptions.unexpectedEnum(mode);

        }
        return error;
    }


    /**
     * @see #startTransaction(TransactionOption, HandleMode)
     */
    private void handleStartTransactionResult(final ResultItem item, final AtomicReference<Isolation> isolationHolder,
                                              final @Nullable Boolean consistentSnapshot) {
        if (item instanceof ResultRow) {
            isolationHolder.set(((ResultRow) item).getNonNull(0, Isolation.class));
        } else if (item instanceof ResultStates && !((ResultStates) item).hasMoreResult()) {
            if (((ResultStates) item).inTransaction()) {
                CURRENT_TX_OPTION.set(this, new CurrentTxOption(isolationHolder.get(), consistentSnapshot));
            } else {
                CURRENT_TX_OPTION.set(this, null);
                throw new JdbdException("transaction start failure"); // no bug,never here
            }
        }
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
        public Publisher<PoolLocalDatabaseSession> reconnect(Duration duration) {
            return this.protocol.reconnect(duration)
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


    private static final class CurrentTxOption {

        private final Isolation isolation;

        private final Boolean consistentSnapshot;

        private CurrentTxOption(@Nullable Isolation isolation, @Nullable Boolean consistentSnapshot) {
            this.isolation = isolation;
            this.consistentSnapshot = consistentSnapshot;
        }

    }// CurrentTxOption


}
