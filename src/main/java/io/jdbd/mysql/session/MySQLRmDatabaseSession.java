package io.jdbd.mysql.session;


import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.*;
import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.result.DataRow;
import io.jdbd.result.ResultItem;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.session.JdbdTransactionStatus;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

/**
 * <p>
 * This class is implementation of {@link RmDatabaseSession} with MySQL client protocol.
 * </p>
 */
class MySQLRmDatabaseSession extends MySQLDatabaseSession<RmDatabaseSession> implements RmDatabaseSession {

    static RmDatabaseSession create(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        return new MySQLRmDatabaseSession(factory, protocol);
    }

    static PoolRmDatabaseSession forPoolVendor(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        return new MySQLPoolRmDatabaseSession(factory, protocol);
    }

    private static final Logger LOG = LoggerFactory.getLogger(MySQLRmDatabaseSession.class);

    private static final AtomicReferenceFieldUpdater<MySQLRmDatabaseSession, XaTxOption> CURRENT_TX_OPTION =
            AtomicReferenceFieldUpdater.newUpdater(MySQLRmDatabaseSession.class, XaTxOption.class, "currentTxOption");

    private static final ConcurrentMap<Xid, XaTxOption> PREPARED_XA_MAP = MySQLCollections.concurrentHashMap();


    private volatile XaTxOption currentTxOption;


    /**
     * private constructor
     */
    private MySQLRmDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        super(factory, protocol);
        protocol.addSessionCloseListener(this::onSessionClose);
        protocol.addTransactionEndListener(this::onTransactionEnd);
    }


    @Override
    public final Publisher<RmDatabaseSession> start(final Xid xid, final int flags) {
        return this.start(xid, flags, TransactionOption.option(null, false));
    }


    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> start(final @Nullable Xid xid, final int flags,
                                                    final @Nullable TransactionOption option) {
        final StringBuilder builder = new StringBuilder(140);
        final Isolation isolation;
        try {

            if (xid == null) {
                throw MySQLExceptions.xidIsNull();
            } else if (option == null) {
                throw MySQLExceptions.xaTransactionOptionIsNull();
            } else if (this.currentTxOption != null || this.inTransaction()) {
                throw MySQLExceptions.xaBusyOnOtherTransaction();
            }

            isolation = option.isolation();
            final String setTransactionSpace = "SET TRANSACTION ";
            if (isolation == null) {
                builder.append("SET @@transaction_isolation =  @@SESSION.transaction_isolation ; ") // here,must guarantee isolation is session isolation
                        .append("SELECT @@SESSION.transaction_isolation AS txIsolationLevel ; ")
                        .append(setTransactionSpace);
            } else {
                builder.append(setTransactionSpace)
                        .append("ISOLATION LEVEL ");
                if (MySQLDatabaseSession.appendIsolation(isolation, builder)) {
                    throw MySQLExceptions.unknownIsolation(isolation);
                }
                builder.append(Constants.SPACE_COMMA_SPACE);
            }

            if (option.isReadOnly()) {
                builder.append("READ ONLY");
            } else {
                builder.append("READ WRITE");
            }

            builder.append(Constants.SPACE_SEMICOLON_SPACE)
                    .append("XA START");

            final XaException error;
            if ((error = xidToString(builder, xid)) != null) {
                throw error;
            } else if ((flags & (~startSupportFlags())) != 0) {
                throw JdbdExceptions.xaInvalidFlagForStart(flags);
            } else if ((flags & TM_JOIN) != 0) {
                builder.append(" JOIN");
            } else if ((flags & TM_RESUME) != 0) {
                builder.append(" RESUME");
            }
        } catch (Throwable e) {
            return Mono.error(MySQLExceptions.wrap(e));
        }

        final AtomicReference<Isolation> isolationHolder = new AtomicReference<>(isolation);
        return Flux.from(this.protocol.staticMultiStmtAsFlux(Stmts.multiStmt(builder.toString())))
                .doOnNext(item -> handleXaStart(item, isolationHolder, xid, flags))
                .doOnError(e -> CURRENT_TX_OPTION.set(this, null))
                .then(Mono.just(this));
    }


    @Override
    public final Publisher<RmDatabaseSession> end(final Xid xid, final int flags) {
        return this.end(xid, flags, DatabaseProtocol.OPTION_FUNC);
    }

    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> end(final Xid xid, final int flags, Function<Option<?>, ?> optionFunc) {

        final XaTxOption option = this.currentTxOption;

        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA END");

        final Mono<RmDatabaseSession> mono;
        final XaException error;
        if (option == null || !option.xid.equals(xid)) {
            mono = Mono.error(MySQLExceptions.xaNonCurrentTransaction(xid)); // here use xid
        } else if (option.xaStates != XaStates.ACTIVE) {
            mono = Mono.error(MySQLExceptions.xaTransactionDontSupportEndCommand(option.xid, option.xaStates));
        } else if (((~endSupportFlags()) & flags) != 0) {
            mono = Mono.error(MySQLExceptions.xaInvalidFlagForEnd(flags));
        } else if ((error = xidToString(builder, option.xid)) != null) { // here use xaTxOption.xid
            mono = Mono.error(error);
        } else {
            if ((flags & TM_SUSPEND) != 0) {
                builder.append(" SUSPEND");
            }
            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .doOnSuccess(states -> CURRENT_TX_OPTION.set(this, new XaTxOption(option.xid, option.isolation, XaStates.IDLE, flags)))
                    .thenReturn(this);
        }
        return mono;
    }

    @Override
    public final Publisher<Integer> prepare(final Xid xid) {
        return this.prepare(xid, DatabaseProtocol.OPTION_FUNC);
    }

    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<Integer> prepare(final Xid xid, final Function<Option<?>, ?> optionFunc) {

        final XaTxOption option = this.currentTxOption;

        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA PREPARE");

        final Mono<Integer> mono;
        final XaException error;
        if (option == null || !option.xid.equals(xid)) {
            mono = Mono.error(MySQLExceptions.xaNonCurrentTransaction(xid)); // here use xid
        } else if (option.xaStates != XaStates.IDLE) {
            mono = Mono.error(MySQLExceptions.xaTransactionDontSupportEndCommand(option.xid, option.xaStates));
        } else if ((error = xidToString(builder, option.xid)) != null) { // here use xaTxOption.xid
            mono = Mono.error(error);
        } else {
            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .doOnSuccess(states -> {
                        CURRENT_TX_OPTION.set(this, null);  // here , couldn't compareAndSet() , because of this.onTransactionEnd();
                        if ((option.flags & TM_FAIL) != 0) { // store  rollback-only.
                            PREPARED_XA_MAP.put(option.xid, new XaTxOption(option.xid, option.isolation, XaStates.PREPARED, option.flags));
                        }
                    })
                    .thenReturn(XA_OK);
        }
        return mono;
    }

    @Override
    public final Publisher<RmDatabaseSession> commit(final Xid xid, final int flags) {
        return this.commit(xid, flags, DatabaseProtocol.OPTION_FUNC);
    }

    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> commit(final @Nullable Xid xid, final int flags,
                                                     Function<Option<?>, ?> optionFunc) {

        final XaTxOption option;

        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA COMMIT");

        final Mono<RmDatabaseSession> mono;
        final XaException error;

        if (flags == TM_ONE_PHASE) {
            if ((option = this.currentTxOption) == null || !option.xid.equals(xid)) {
                mono = Mono.error(MySQLExceptions.xaNonCurrentTransaction(xid)); // here use xid
            } else if (option.xaStates != XaStates.IDLE) {
                mono = Mono.error(MySQLExceptions.xaStatesDontSupportCommitCommand(option.xid, option.xaStates));
            } else if ((option.flags & TM_FAIL) != 0) {
                mono = Mono.error(MySQLExceptions.xaTransactionRollbackOnly(xid));
            } else if ((error = xidToString(builder, option.xid)) != null) { // here use xaTxOption.xid
                mono = Mono.error(error);
            } else {
                builder.append(" ONE PHASE");
                mono = this.protocol.update(Stmts.stmt(builder.toString()))
                        .doOnSuccess(states -> CURRENT_TX_OPTION.set(this, null)) // here , couldn't compareAndSet() , because of this.onTransactionEnd();
                        .thenReturn(this);
            }
        } else if (flags != TM_NO_FLAGS) {
            mono = Mono.error(MySQLExceptions.xaInvalidFlagForEnd(flags));
        } else if ((option = PREPARED_XA_MAP.get(xid)) != null && (option.flags & TM_FAIL) != 0) {
            mono = Mono.error(MySQLExceptions.xaTransactionRollbackOnly(xid));
        } else if ((error = xidToString(builder, xid)) != null) { // here use xid
            mono = Mono.error(error);
        } else {
            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .doOnSuccess(states -> PREPARED_XA_MAP.remove(xid))
                    .thenReturn(this);
        }
        return mono;
    }

    @Override
    public final Publisher<RmDatabaseSession> rollback(final Xid xid) {
        return this.rollback(xid, DatabaseProtocol.OPTION_FUNC);
    }


    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> rollback(final Xid xid, final Function<Option<?>, ?> optionFunc) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA ROLLBACK");

        final XaException error;
        final Mono<RmDatabaseSession> mono;
        if ((error = xidToString(builder, xid)) != null) { // here use xid
            mono = Mono.error(error);
        } else {
            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .doOnSuccess(states -> PREPARED_XA_MAP.remove(xid))
                    .thenReturn(this);
        }
        return mono;
    }


    @Override
    public final Publisher<RmDatabaseSession> forget(final Xid xid) {
        return this.forget(xid, DatabaseProtocol.OPTION_FUNC);
    }


    @Override
    public final Publisher<RmDatabaseSession> forget(Xid xid, Function<Option<?>, ?> optionFunc) {
        return Mono.error(new XaException("MySQL don't support forget command", XaException.XAER_RMERR));
    }

    @Override
    public final Publisher<Optional<Xid>> recover(final int flags) {
        return this.recover(flags, DatabaseProtocol.OPTION_FUNC);
    }


    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<Optional<Xid>> recover(final int flags, final @Nullable Function<Option<?>, ?> optionFunc) {
        final Flux<Optional<Xid>> flux;
        if (optionFunc == null) {
            flux = Flux.error(new NullPointerException());
        } else if ((flags & (~(TM_START_RSCAN | TM_END_RSCAN))) != 0) {
            flux = Flux.error(MySQLExceptions.xaInvalidFlagForRecover(flags));
        } else if ((flags & TM_START_RSCAN) != 0) {
            flux = Flux.empty();
        } else {
            flux = this.protocol.query(Stmts.stmt("XA RECOVER CONVERT XID"), this::mapRecoverResult,
                    DatabaseProtocol.IGNORE_RESULT_STATES);
        }
        return flux;
    }


    @Override
    public final boolean isSupportForget() {
        // false,MySQL don't support forget method
        return false;
    }

    @Override
    public final int startSupportFlags() {
        return (TM_RESUME | TM_JOIN | TM_NO_FLAGS);
    }

    @Override
    public final int endSupportFlags() {
        return (TM_SUCCESS | TM_FAIL | TM_SUSPEND);
    }

    @Override
    public final int recoverSupportFlags() {
        return (TM_START_RSCAN | TM_END_RSCAN | TM_NO_FLAGS);
    }


    /**
     * @see #transactionStatus()
     */
    @Override
    final Mono<TransactionStatus> mapTransactionStatus(final List<ResultItem> list) {
        final ResultRow row;
        final ResultStates states;
        row = (ResultRow) list.get(0);
        states = (ResultStates) list.get(1);


        final Boolean readOnly;
        final XaTxOption xaTxOption;

        final Mono<TransactionStatus> mono;
        if ((readOnly = states.valueOf(Option.READ_ONLY)) == null) {
            // no bug,never here
            mono = Mono.error(new JdbdException("result status no read only"));
        } else if (!states.inTransaction()) {
            // session transaction characteristic
            final Isolation isolation;
            isolation = row.getNonNull(0, Isolation.class);
            mono = Mono.just(JdbdTransactionStatus.txStatus(isolation, row.getNonNull(1, Boolean.class), false));
        } else if ((xaTxOption = this.currentTxOption) == null) {
            String m = "Not found cache current transaction option,you dont use jdbd-spi to control transaction.";
            mono = Mono.error(new XaException(m, null, 0, XaException.XAER_PROTO));
        } else {
            final Map<Option<?>, Object> map = MySQLCollections.hashMap(11);

            map.put(Option.XID, xaTxOption.xid);
            map.put(Option.IN_TRANSACTION, Boolean.TRUE);
            map.put(Option.ISOLATION, xaTxOption.isolation); // MySQL don't support get current isolation level
            map.put(Option.READ_ONLY, readOnly);

            map.put(Option.XA_STATES, xaTxOption.xaStates);
            map.put(Option.XA_FLAGS, xaTxOption.flags);
            mono = Mono.just(JdbdTransactionStatus.fromMap(map));
        }
        return mono;
    }

    @Override
    final void printTransactionInfo(final StringBuilder builder) {
        final XaTxOption xaTxOption = this.currentTxOption;
        if (xaTxOption != null) {
            builder.append(" , currentTransactionXid : ")
                    .append(xaTxOption.xid)
                    .append(" , currentTransactionXaStates : ")
                    .append(xaTxOption.xaStates.name())
                    .append(" , currentTransactionIsolation : ")
                    .append(xaTxOption.isolation.name())
                    .append(" , currentTransactionFlags : ")
                    .append(xaTxOption.flags);
        }
    }

    /**
     * @see #start(Xid, int, TransactionOption)
     */
    private void handleXaStart(final ResultItem item, final AtomicReference<Isolation> isolationHolder,
                               final Xid xid, final int flags) {
        if (item instanceof ResultRow) {
            isolationHolder.set(((ResultRow) item).getNonNull(0, Isolation.class));
        } else if (item instanceof ResultStates && !((ResultStates) item).hasMoreResult()) {
            if (((ResultStates) item).inTransaction()) {
                CURRENT_TX_OPTION.set(this, new XaTxOption(xid, isolationHolder.get(), XaStates.ACTIVE, flags)); // NOTE : here occur after this.onTransactionEnd();
            } else {
                CURRENT_TX_OPTION.set(this, null);
                throw new JdbdException("transaction start failure"); // no bug,never here
            }
        }
    }


    final void onSessionClose() {
        super.onSessionClose();
        CURRENT_TX_OPTION.set(this, null);  // clear cache , avoid reconnect occur bug
        LOG.debug("session close event,clear current xa transaction cache.");

    }

    private void onTransactionEnd() {
        CURRENT_TX_OPTION.set(this, null); // clear cache,avoid bug
        LOG.debug("transaction end event,clear current xa transaction cache.");
    }


    /**
     * @see #recover(int, Function)
     */
    private Optional<Xid> mapRecoverResult(final DataRow row) {
        final int formatId, gtridLength, bqualLength;

        formatId = row.getNonNull(0, Integer.class); // formatID
        gtridLength = row.getNonNull(1, Integer.class); // gtrid_length
        bqualLength = row.getNonNull(2, Integer.class); // bqual_length

        final String hexString;
        hexString = row.getNonNull(3, String.class); // data

        assert hexString.startsWith("0x") : "mysql XA RECOVER convert xid response error";

        final byte[] idBytes;
        idBytes = MySQLBuffers.decodeHex(hexString.substring(2).getBytes(StandardCharsets.UTF_8));

        final String gtrid, bqual;
        if (gtridLength == 0) {
            return Optional.empty(); // non-jdbd create xid
        }
        gtrid = new String(idBytes, 0, gtridLength);
        if (bqualLength == 0) {
            bqual = null;
        } else {
            bqual = new String(idBytes, gtridLength, bqualLength);
        }
        return Optional.of(Xid.from(gtrid, bqual, formatId));
    }


    /**
     * @see #start(Xid, int, TransactionOption)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Nullable
    private static XaException xidToString(final StringBuilder builder, final @Nullable Xid xid) {
        if (xid == null) {
            return MySQLExceptions.xidIsNull();
        }
        final String gtrid, bqual;
        gtrid = xid.getGtrid();
        bqual = xid.getBqual();

        final byte[] gtridBytes, bqualBytes, formatIdBytes;

        if (!MySQLStrings.hasText(gtrid)) {
            return MySQLExceptions.xaGtridNoText();
        } else if ((gtridBytes = gtrid.getBytes(StandardCharsets.UTF_8)).length > 64) {
            return MySQLExceptions.xaGtridBeyond64Bytes();
        }

        builder.append(" 0x")
                .append(MySQLBuffers.hexEscapesText(true, gtridBytes, gtridBytes.length));

        builder.append(Constants.COMMA);
        if (bqual != null) {
            if ((bqualBytes = bqual.getBytes(StandardCharsets.UTF_8)).length > 64) {
                return MySQLExceptions.xaBqualBeyond64Bytes();
            }
            builder.append("0x")
                    .append(MySQLBuffers.hexEscapesText(true, bqualBytes, bqualBytes.length));
        }
        final int formatId;
        formatId = xid.getFormatId();

        builder.append(",0x");
        if (formatId == 0) {
            builder.append('0');
        } else {
            formatIdBytes = MySQLNumbers.toBinaryBytes(formatId, true);
            int offset = 0;
            for (; offset < formatIdBytes.length; offset++) {
                if (formatIdBytes[offset] != 0) {
                    break;
                }
            }
            builder.append(MySQLBuffers.hexEscapesText(true, formatIdBytes, offset, formatIdBytes.length));
        }
        return null;
    }


    /**
     * <p>
     * This class is implementation of {@link PoolRmDatabaseSession} with MySQL client protocol.
     * </p>
     */
    private static final class MySQLPoolRmDatabaseSession extends MySQLRmDatabaseSession
            implements PoolRmDatabaseSession {

        private MySQLPoolRmDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
            super(factory, protocol);
        }


        @Override
        public Mono<PoolRmDatabaseSession> ping(int timeoutSeconds) {
            return this.protocol.ping(timeoutSeconds)
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolRmDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }


    }// MySQLPoolRmDatabaseSession

    private static final class XaTxOption {

        private final Xid xid;

        private final Isolation isolation;

        private final XaStates xaStates;

        private final int flags;

        private XaTxOption(Xid xid, Isolation isolation, XaStates xaStates, int flags) {
            this.xid = xid;
            this.isolation = isolation;
            this.xaStates = xaStates;
            this.flags = flags;
        }


    }// XaTxOption

}
