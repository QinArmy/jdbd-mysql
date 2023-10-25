package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.pool.PoolLocalDatabaseSession;
import io.jdbd.result.ResultItem;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.vendor.session.JdbdTransactionInfo;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

/**
 * <p>
 * This class is implementation of {@link LocalDatabaseSession} with MySQL client protocol.
 * <br/>
 */
class MySQLLocalDatabaseSession extends MySQLDatabaseSession<LocalDatabaseSession> implements LocalDatabaseSession {


    static LocalDatabaseSession create(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        return new MySQLLocalDatabaseSession(factory, protocol);
    }

    static PoolLocalDatabaseSession forPoolVendor(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        return new MySQLPoolLocalDatabaseSession(factory, protocol);
    }

    private static final Logger LOG = LoggerFactory.getLogger(MySQLLocalDatabaseSession.class);

    private static final AtomicReferenceFieldUpdater<MySQLLocalDatabaseSession, TransactionInfo> TRANSACTION_INFO =
            AtomicReferenceFieldUpdater.newUpdater(MySQLLocalDatabaseSession.class, TransactionInfo.class, "transactionInfo");


    private volatile TransactionInfo transactionInfo;


    /**
     * private constructor
     */
    private MySQLLocalDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        super(factory, protocol);
        protocol.addSessionCloseListener(this::onSessionClose);
        protocol.addTransactionEndListener(this::onTransactionEnd);
    }

    @Override
    public Publisher<TransactionInfo> startTransaction() {
        return this.startTransaction(TransactionOption.option(null, false), HandleMode.ERROR_IF_EXISTS);
    }

    @Override
    public final Publisher<TransactionInfo> startTransaction(TransactionOption option) {
        return this.startTransaction(option, HandleMode.ERROR_IF_EXISTS);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/commit.html">START TRANSACTION Statement</a>
     */
    @Override
    public final Publisher<TransactionInfo> startTransaction(final @Nullable TransactionOption option,
                                                             final @Nullable HandleMode mode) {
        if (option == null || mode == null) {
            return Mono.error(new NullPointerException());
        }

        final StringBuilder builder = new StringBuilder(168);
        final TransactionInfo info = this.transactionInfo;
        final JdbdException error;
        if ((info != null || this.inTransaction())
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
        return Flux.from(this.protocol.staticMultiStmtAsFlux(Stmts.multiStmt(builder.toString())))
                .doOnNext(item -> handleStartTransactionResult(item, isolationHolder, consistentSnapshot))
                .then(Mono.defer(this::getTransactionInfoAfterStart))
                .doOnError(e -> TRANSACTION_INFO.set(this, null));
    }


    @Override
    public final Publisher<LocalDatabaseSession> commit() {
        return this.commit(Option.EMPTY_OPTION_FUNC);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/commit.html">COMMIT</a>
     */
    @Override
    public final Publisher<LocalDatabaseSession> commit(final @Nullable Function<Option<?>, ?> optionFunc) {
        return commitOrRollback(true, optionFunc);
    }

    @Override
    public final Publisher<LocalDatabaseSession> rollback() {
        return this.rollback(Option.EMPTY_OPTION_FUNC);
    }

    @Override
    public final Publisher<LocalDatabaseSession> rollback(Function<Option<?>, ?> optionFunc) {
        return commitOrRollback(false, optionFunc);
    }


    /**
     * @see #transactionInfo()
     */
    final Mono<TransactionInfo> mapTransactionStatus(final List<ResultItem> list) {
        final ResultRow row;
        final ResultStates states;
        row = (ResultRow) list.get(0);
        states = (ResultStates) list.get(1);

        final TransactionInfo info;

        final Mono<TransactionInfo> mono;
        if (!states.inTransaction()) {
            // session transaction characteristic
            final Isolation isolation;
            isolation = row.getNonNull(0, Isolation.class);
            mono = Mono.just(JdbdTransactionInfo.txInfo(isolation, row.getNonNull(1, Boolean.class), false));
        } else if ((info = this.transactionInfo) == null) {
            String m = "Not found cache current transaction option,you dont use jdbd-spi to control transaction.";
            mono = Mono.error(new JdbdException(m));
        } else {
            mono = Mono.just(info);
        }
        return mono;
    }


    @Override
    final void printTransactionInfo(final StringBuilder builder) {
        final TransactionInfo info = this.transactionInfo;
        if (info != null) {
            builder.append(" , currentTransactionIsolation : ")
                    .append(info.isolation().name())
                    .append(" , currentTransactionConsistentSnapshot : ")
                    .append(info.valueOf(Option.WITH_CONSISTENT_SNAPSHOT));
        }
    }

    /**
     * @see #commit(Function)
     * @see #rollback(Function)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/commit.html">COMMIT</a>
     */
    private Mono<LocalDatabaseSession> commitOrRollback(final boolean commit,
                                                        final @Nullable Function<Option<?>, ?> optionFunc) {
        if (optionFunc == null) {
            return Mono.error(new NullPointerException());
        }
        final Object chain, release;
        chain = optionFunc.apply(Option.CHAIN);
        release = optionFunc.apply(Option.RELEASE);

        if (Boolean.TRUE.equals(chain) && Boolean.TRUE.equals(release)) {
            String m = String.format("%s[true] and %s[true] conflict", Option.CHAIN.name(), Option.RELEASE.name());
            return Mono.error(new JdbdException(m));
        }

        final StringBuilder builder = new StringBuilder(20);
        if (commit) {
            builder.append(COMMIT);
        } else {
            builder.append(ROLLBACK);
        }
        if (chain instanceof Boolean) {
            builder.append(" AND");
            if (!((Boolean) chain)) {
                builder.append(" NO");
            }
            builder.append(" CHAIN");
        }

        if (release instanceof Boolean) {
            if (!((Boolean) release)) {
                builder.append(" NO");
            }
            builder.append(" RELEASE");
        }
        final TransactionInfo info = this.transactionInfo;
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .doOnSuccess(states -> {
                    if (states.inTransaction()) {
                        assert Boolean.TRUE.equals(chain) : "ResultStates bug";
                        TRANSACTION_INFO.set(this, info); // record old value. NOTE : this occur after this.onTransactionEnd().
                    } else {
                        TRANSACTION_INFO.set(this, null);
                    }
                })
                .thenReturn(this);
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
            isolationHolder.compareAndSet(null, ((ResultRow) item).getNonNull(0, Isolation.class));
        } else if (item instanceof ResultStates && !((ResultStates) item).hasMoreResult()) {
            final ResultStates states = (ResultStates) item;
            if (states.inTransaction()) {
                final boolean readOnly = states.nonNullOf(Option.READ_ONLY);
                final TransactionInfo info;
                info = createTransactionInfoAfterStart(isolationHolder, readOnly, consistentSnapshot);
                TRANSACTION_INFO.set(this, info);
            } else {
                TRANSACTION_INFO.set(this, null);
                throw new JdbdException("transaction start failure"); // no bug,never here
            }
        }
    }

    /**
     * @see #startTransaction(TransactionOption, HandleMode)
     */
    private Mono<TransactionInfo> getTransactionInfoAfterStart() {
        final TransactionInfo info = TRANSACTION_INFO.get(this);
        Mono<TransactionInfo> mono;
        if (info == null) {
            mono = Mono.error(MySQLExceptions.concurrentStartTransaction());
        } else {
            mono = Mono.just(info);
        }
        return mono;
    }


    /**
     * @see #handleStartTransactionResult(ResultItem, AtomicReference, Boolean)
     */
    private TransactionInfo createTransactionInfoAfterStart(final AtomicReference<Isolation> isolationHolder,
                                                            final boolean readOnly,
                                                            final @Nullable Boolean consistentSnapshot) {
        final TransactionInfo info;
        if (consistentSnapshot == null) {
            info = JdbdTransactionInfo.txInfo(isolationHolder.get(), readOnly, true);
        } else {
            final Map<Option<?>, Object> map = MySQLCollections.hashMap(7);
            map.put(Option.ISOLATION, isolationHolder.get());
            map.put(Option.READ_ONLY, readOnly);
            map.put(Option.IN_TRANSACTION, Boolean.TRUE);
            map.put(Option.WITH_CONSISTENT_SNAPSHOT, consistentSnapshot);
            info = JdbdTransactionInfo.fromMap(map);
        }
        return info;
    }


    final void onSessionClose() {
        super.onSessionClose();
        TRANSACTION_INFO.set(this, null); // clear cache , avoid reconnect occur bug
        LOG.debug("session close event,clear current local transaction cache.");
        ;

    }

    private void onTransactionEnd() {
        TRANSACTION_INFO.set(this, null); // clear cache,avoid bug
        LOG.debug("transaction end event,clear current local transaction cache.");
    }


    /**
     * <p>
     * This class is implementation of {@link PoolLocalDatabaseSession} with MySQL client protocol.
     * <br/>
     *
     * @since 1.0
     */
    private static final class MySQLPoolLocalDatabaseSession extends MySQLLocalDatabaseSession
            implements PoolLocalDatabaseSession {

        private MySQLPoolLocalDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
            super(factory, protocol);
        }


        @Override
        public Mono<PoolLocalDatabaseSession> ping() {
            return this.protocol.ping()
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolLocalDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }

        @Override
        public Publisher<PoolLocalDatabaseSession> logicallyClose() {
            return this.protocol.logicallyClose()
                    .thenReturn(this);
        }


    }// MySQLPoolLocalDatabaseSession


    private static final class LocalTxOption {

        private final Isolation isolation;

        private final Boolean consistentSnapshot;

        private LocalTxOption(@Nullable Isolation isolation, @Nullable Boolean consistentSnapshot) {
            this.isolation = isolation;
            this.consistentSnapshot = consistentSnapshot;
        }

    }// CurrentTxOption


}
