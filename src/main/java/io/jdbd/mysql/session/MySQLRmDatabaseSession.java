package io.jdbd.mysql.session;


import io.jdbd.JdbdException;
import io.jdbd.lang.NonNull;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLNumbers;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.result.DataRow;
import io.jdbd.result.ResultItem;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.util.JdbdUtils;
import io.jdbd.vendor.session.JdbdTransactionInfo;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

/**
 * <p>
 * This class is implementation of {@link RmDatabaseSession} with MySQL client protocol.
 * <br/>
 */
class MySQLRmDatabaseSession extends MySQLDatabaseSession<RmDatabaseSession> implements RmDatabaseSession {

    static RmDatabaseSession create(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        return new MySQLRmDatabaseSession(factory, protocol);
    }

    static PoolRmDatabaseSession forPoolVendor(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        return new MySQLPoolRmDatabaseSession(factory, protocol);
    }

    private static final Logger LOG = LoggerFactory.getLogger(MySQLRmDatabaseSession.class);

    private static final AtomicReferenceFieldUpdater<MySQLRmDatabaseSession, XaTransactionInfo> TRANSACTION_INFO =
            AtomicReferenceFieldUpdater.newUpdater(MySQLRmDatabaseSession.class, XaTransactionInfo.class, "transactionInfo");

    private static final ConcurrentMap<Xid, XaTransactionInfo> PREPARED_XA_MAP = MySQLCollections.concurrentHashMap();


    private volatile XaTransactionInfo transactionInfo;


    /**
     * private constructor
     */
    private MySQLRmDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        super(factory, protocol);
        protocol.addSessionCloseListener(this::onSessionClose);
        protocol.addTransactionEndListener(this::onTransactionEnd);
    }


    @Override
    public final Publisher<TransactionInfo> start(Xid xid) {
        return this.start(xid, TM_NO_FLAGS, TransactionOption.option(null, false));
    }

    @Override
    public final Publisher<TransactionInfo> start(final Xid xid, final int flags) {
        return this.start(xid, flags, TransactionOption.option(null, false));
    }


    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * <br/>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<TransactionInfo> start(final @Nullable Xid xid, final int flags,
                                                  final @Nullable TransactionOption option) {
        final StringBuilder builder = new StringBuilder(140);
        final Isolation isolation;
        try {

            if (xid == null) {
                throw MySQLExceptions.xidIsNull();
            } else if (option == null) {
                throw MySQLExceptions.xaTransactionOptionIsNull();
            } else if (this.transactionInfo != null || this.inTransaction()) {
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
                .then(Mono.defer(this::getTransactionInfoAfterStart))
                .doOnError(e -> TRANSACTION_INFO.set(this, null));
    }


    @Override
    public final Publisher<RmDatabaseSession> end(Xid xid) {
        return this.end(xid, TM_SUCCESS, Option.EMPTY_OPTION_FUNC);
    }

    @Override
    public final Publisher<RmDatabaseSession> end(final Xid xid, final int flags) {
        return this.end(xid, flags, Option.EMPTY_OPTION_FUNC);
    }

    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * <br/>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> end(final Xid xid, final int flags, Function<Option<?>, ?> optionFunc) {

        final XaTransactionInfo info = this.transactionInfo;

        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA END");

        final Mono<RmDatabaseSession> mono;
        final XaException error;
        if (info == null || !info.xid.equals(xid)) {
            mono = Mono.error(MySQLExceptions.xaNonCurrentTransaction(xid)); // here use xid
        } else if (info.xaStates != XaStates.ACTIVE) {
            mono = Mono.error(MySQLExceptions.xaTransactionDontSupportEndCommand(info.xid, info.xaStates));
        } else if (((~endSupportFlags()) & flags) != 0) {
            mono = Mono.error(MySQLExceptions.xaInvalidFlagForEnd(flags));
        } else if ((error = xidToString(builder, info.xid)) != null) { // here use xaTxOption.xid
            mono = Mono.error(error);
        } else {
            if ((flags & TM_SUSPEND) != 0) {
                builder.append(" SUSPEND");
            }
            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .doOnSuccess(states -> TRANSACTION_INFO.set(this, new XaTransactionInfo(info, XaStates.IDLE, flags)))
                    .thenReturn(this);
        }
        return mono;
    }

    @Override
    public final Publisher<Integer> prepare(final Xid xid) {
        return this.prepare(xid, Option.EMPTY_OPTION_FUNC);
    }

    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * <br/>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<Integer> prepare(final Xid xid, final Function<Option<?>, ?> optionFunc) {

        final XaTransactionInfo info = this.transactionInfo;

        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA PREPARE");

        final Mono<Integer> mono;
        final XaException error;
        if (info == null || !info.xid.equals(xid)) {
            mono = Mono.error(MySQLExceptions.xaNonCurrentTransaction(xid)); // here use xid
        } else if (info.xaStates != XaStates.IDLE) {
            mono = Mono.error(MySQLExceptions.xaStatesDontSupportPrepareCommand(info.xid, info.xaStates));
        } else if ((error = xidToString(builder, info.xid)) != null) { // here use xaTxOption.xid
            mono = Mono.error(error);
        } else {
            mono = this.protocol.update(Stmts.stmt(builder.toString()))
                    .doOnSuccess(states -> {
                        TRANSACTION_INFO.set(this, null);  // here , couldn't compareAndSet() , because of this.onTransactionEnd();
                        if ((info.flags & TM_FAIL) != 0) { // store  rollback-only.
                            PREPARED_XA_MAP.put(info.xid, new XaTransactionInfo(info, XaStates.PREPARED, info.flags));
                        }
                    })
                    .thenReturn(XA_OK);
        }
        return mono;
    }

    @Override
    public final Publisher<RmDatabaseSession> commit(Xid xid) {
        return this.commit(xid, TM_NO_FLAGS, Option.EMPTY_OPTION_FUNC);
    }

    @Override
    public final Publisher<RmDatabaseSession> commit(final Xid xid, final int flags) {
        return this.commit(xid, flags, Option.EMPTY_OPTION_FUNC);
    }

    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * <br/>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> commit(final @Nullable Xid xid, final int flags,
                                                     Function<Option<?>, ?> optionFunc) {

        final XaTransactionInfo info;

        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA COMMIT");

        final Mono<RmDatabaseSession> mono;
        final XaException error;

        if (flags == TM_ONE_PHASE) {
            if ((info = this.transactionInfo) == null || !info.xid.equals(xid)) {
                mono = Mono.error(MySQLExceptions.xaNonCurrentTransaction(xid)); // here use xid
            } else if (info.xaStates != XaStates.IDLE) {
                mono = Mono.error(MySQLExceptions.xaStatesDontSupportCommitCommand(info.xid, info.xaStates));
            } else if ((info.flags & TM_FAIL) != 0) {
                mono = Mono.error(MySQLExceptions.xaTransactionRollbackOnly(xid));
            } else if ((error = xidToString(builder, info.xid)) != null) { // here use xaTxOption.xid
                mono = Mono.error(error);
            } else {
                builder.append(" ONE PHASE");
                mono = this.protocol.update(Stmts.stmt(builder.toString()))
                        .doOnSuccess(states -> TRANSACTION_INFO.set(this, null)) // here , couldn't compareAndSet() , because of this.onTransactionEnd();
                        .thenReturn(this);
            }
        } else if (flags != TM_NO_FLAGS) {
            mono = Mono.error(MySQLExceptions.xaInvalidFlagForEnd(flags));
        } else if ((info = PREPARED_XA_MAP.get(xid)) != null && (info.flags & TM_FAIL) != 0) {
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
        return this.rollback(xid, Option.EMPTY_OPTION_FUNC);
    }


    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * <br/>
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
        return this.forget(xid, Option.EMPTY_OPTION_FUNC);
    }


    @Override
    public final Publisher<RmDatabaseSession> forget(Xid xid, Function<Option<?>, ?> optionFunc) {
        return Mono.error(new XaException("MySQL don't support forget command", XaException.XAER_RMERR));
    }

    @Override
    public final Publisher<Optional<Xid>> recover() {
        return this.recover(TM_NO_FLAGS, Option.EMPTY_OPTION_FUNC);
    }

    @Override
    public final Publisher<Optional<Xid>> recover(final int flags) {
        return this.recover(flags, Option.EMPTY_OPTION_FUNC);
    }


    /**
     * <p>
     * the conversion process of xid is same with MySQL Connector/J .
     * <br/>
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
                    ResultStates.IGNORE_STATES);
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
     * @see #transactionInfo()
     */
    @Override
    final Mono<TransactionInfo> mapTransactionStatus(final List<ResultItem> list) {
        final ResultRow row;
        final ResultStates states;
        row = (ResultRow) list.get(0);
        states = (ResultStates) list.get(1);
        final XaTransactionInfo info;

        final Mono<TransactionInfo> mono;
        if (!states.inTransaction()) {
            // session transaction characteristic
            final Isolation isolation;
            isolation = row.getNonNull(0, Isolation.class);
            mono = Mono.just(JdbdTransactionInfo.txInfo(isolation, row.getNonNull(1, Boolean.class), false));
        } else if ((info = this.transactionInfo) == null) {
            String m = "Not found cache current transaction info,you dont use jdbd-spi to control transaction.";
            mono = Mono.error(new XaException(m, null, 0, XaException.XAER_PROTO));
        } else {
            mono = Mono.just(info);
        }
        return mono;
    }

    @Override
    final void printTransactionInfo(final StringBuilder builder) {
        final XaTransactionInfo info = this.transactionInfo;
        if (info != null) {
            builder.append(" , currentTransactionXid : ")
                    .append(info.xid)
                    .append(" , currentTransactionXaStates : ")
                    .append(info.xaStates.name())
                    .append(" , currentTransactionIsolation : ")
                    .append(info.isolation.name())
                    .append(" , currentTransactionFlags : ")
                    .append(info.flags);
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
            final ResultStates states = (ResultStates) item;
            if (states.inTransaction()) {
                final boolean readOnly = states.nonNullOf(Option.READ_ONLY);
                final XaTransactionInfo info;
                info = new XaTransactionInfo(isolationHolder.get(), readOnly, xid, flags);
                TRANSACTION_INFO.set(this, info); // NOTE : here occur after this.onTransactionEnd();
            } else {
                TRANSACTION_INFO.set(this, null);
                throw new JdbdException("transaction start failure"); // no bug,never here
            }
        }
    }

    /**
     * @see #start(Xid, int, TransactionOption)
     */
    private Mono<TransactionInfo> getTransactionInfoAfterStart() {
        final TransactionInfo info;
        info = TRANSACTION_INFO.get(this);
        Mono<TransactionInfo> mono;
        if (info == null) {
            mono = Mono.error(MySQLExceptions.concurrentStartTransaction());
        } else {
            mono = Mono.just(info);
        }
        return mono;
    }


    final void onSessionClose() {
        super.onSessionClose();
        TRANSACTION_INFO.set(this, null);  // clear cache , avoid reconnect occur bug
        LOG.debug("session close event,clear current xa transaction cache.");

    }

    private void onTransactionEnd() {
        TRANSACTION_INFO.set(this, null); // clear cache,avoid bug
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
        idBytes = JdbdUtils.decodeHex(hexString.substring(2).getBytes(StandardCharsets.UTF_8));

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
                .append(JdbdUtils.hexEscapesText(true, gtridBytes, gtridBytes.length));

        builder.append(Constants.COMMA);
        if (bqual != null) {
            if ((bqualBytes = bqual.getBytes(StandardCharsets.UTF_8)).length > 64) {
                return MySQLExceptions.xaBqualBeyond64Bytes();
            }
            builder.append("0x")
                    .append(JdbdUtils.hexEscapesText(true, bqualBytes, bqualBytes.length));
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
            builder.append(JdbdUtils.hexEscapesText(true, formatIdBytes, offset, formatIdBytes.length));
        }
        return null;
    }


    /**
     * <p>
     * This class is implementation of {@link PoolRmDatabaseSession} with MySQL client protocol.
     * <br/>
     */
    private static final class MySQLPoolRmDatabaseSession extends MySQLRmDatabaseSession
            implements PoolRmDatabaseSession {

        private MySQLPoolRmDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
            super(factory, protocol);
        }


        @Override
        public Mono<PoolRmDatabaseSession> ping() {
            return this.protocol.ping()
                    .thenReturn(this);
        }

        @Override
        public Mono<PoolRmDatabaseSession> reset() {
            return this.protocol.reset()
                    .thenReturn(this);
        }

        @Override
        public Publisher<PoolRmDatabaseSession> logicallyClose() {
            return this.protocol.logicallyClose()
                    .thenReturn(this);
        }

    }// MySQLPoolRmDatabaseSession


    private static final class XaTransactionInfo implements TransactionInfo {

        private final Isolation isolation;

        private final boolean readOnly;

        private final Xid xid;

        private final int flags;

        private final XaStates xaStates;

        private XaTransactionInfo(Isolation isolation, boolean readOnly, Xid xid, int flags) {
            this.isolation = isolation;
            this.readOnly = readOnly;
            this.xid = xid;
            this.flags = flags;
            this.xaStates = XaStates.ACTIVE;
        }

        private XaTransactionInfo(XaTransactionInfo info, XaStates states, int flags) {
            assert states != XaStates.ACTIVE;
            this.isolation = info.isolation;
            this.readOnly = info.readOnly;
            this.xid = info.xid;

            this.xaStates = states;
            this.flags = flags;
        }

        @NonNull
        @Override
        public Isolation isolation() {
            return this.isolation;
        }

        @Override
        public boolean isReadOnly() {
            return this.readOnly;
        }

        @Override
        public boolean inTransaction() {
            final boolean in;
            switch (this.xaStates) {
                case ACTIVE:
                case IDLE:
                    in = true;
                    break;
                case PREPARED:
                default:
                    in = false;
            }
            return in;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T valueOf(final Option<T> option) {
            final Object value;
            if (option == Option.XID) {
                value = this.xid;
            } else if (option == Option.XA_STATES) {
                value = this.xaStates;
            } else if (option == Option.XA_FLAGS) {
                value = this.flags;
            } else if (option == Option.ISOLATION) {
                value = this.isolation;
            } else if (option == Option.READ_ONLY) {
                value = this.readOnly;
            } else if (option == Option.IN_TRANSACTION) {
                value = inTransaction();
            } else {
                value = null;
            }
            return (T) value;
        }


        @Override
        public String toString() {
            return MySQLStrings.builder(68)
                    .append(getClass().getName())
                    .append("[ xid : ")
                    .append(this.xid)
                    .append(" , xaStates : ")
                    .append(this.xaStates.name())
                    .append(" , flags : ")
                    .append(this.flags)
                    .append(" , isolation : ")
                    .append(this.isolation.name())
                    .append(" , readOnly : ")
                    .append(this.readOnly)
                    .append(" , inTransaction : ")
                    .append(this.inTransaction())
                    .append(" ]")
                    .toString();
        }


    }// MySqlXaTransactionInfo

}
