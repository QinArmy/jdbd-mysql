package io.jdbd.mysql.session;


import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLBuffers;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLProtocolUtil;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.session.XidImpl;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
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


    /**
     * private constructor
     */
    private MySQLRmDatabaseSession(MySQLDatabaseSessionFactory factory, MySQLProtocol protocol) {
        super(factory, protocol);
    }


    @Override
    public final Publisher<RmDatabaseSession> start(final Xid xid, final int flags) {
        return this.start(xid, flags, TransactionOption.option(null, false));
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> start(final @Nullable Xid xid, final int flags, final TransactionOption option) {
        final StringBuilder builder = new StringBuilder(140);

        try {
            builder.append("SET TRANSACTION ISOLATION LEVEL ");
            final Isolation isolation = option.isolation();
            if (isolation != null) {
                if (MySQLProtocolUtil.appendIsolation(isolation, builder)) {
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

            xidToString(builder, xid);
            switch (flags) {
                case TM_JOIN:
                    builder.append(" JOIN");
                    break;
                case TM_RESUME:
                    builder.append(" RESUME");
                    break;
                case TM_NO_FLAGS:
                    // no-op
                    break;
                default:
                    throw JdbdExceptions.xaInvalidFlagForStart(flags);
            }
        } catch (Throwable e) {
            return Mono.error(MySQLExceptions.wrap(e));
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .thenReturn(this);
    }


    @Override
    public final Publisher<RmDatabaseSession> end(final Xid xid, final int flags) {
        return this.end(xid, flags, DatabaseProtocol.OPTION_FUNC);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> end(final Xid xid,final int flags, Function<Option<?>, ?> optionFunc) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA END");
        try {
            xidToString(builder, xid);
            switch (flags) {
                case TM_SUCCESS:
                case TM_FAIL:
                    //no-op
                    break;
                case TM_SUSPEND:
                    builder.append(" SUSPEND");
                    break;
                default:
                    throw JdbdExceptions.xaInvalidFlagForEnd(flags);
            }
        } catch (Throwable e) {
            return Mono.error(MySQLExceptions.wrap(e));
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .thenReturn(this);
    }

    @Override
    public final Mono<Integer> prepare(final Xid xid) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA PREPARE");
        try {
            xidToString(builder, xid);
        } catch (Throwable e) {
            return Mono.error(e);
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .map(this::mapPrepareResultCode);

    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<Integer> prepare(Xid xid, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public final Mono<RmDatabaseSession> commit(Xid xid, final boolean onePhase) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA COMMIT");
        try {
            xidToString(builder, xid);
        } catch (Throwable e) {
            return Mono.error(e);
        }
        if (onePhase) {
            builder.append(" ONE PHASE");
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .map(this::mapCommitResult)
                .thenReturn(this);
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> commit(Xid xid, boolean onePhase, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public final Mono<RmDatabaseSession> rollback(final Xid xid) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA ROLLBACK");
        try {
            xidToString(builder, xid);
        } catch (Throwable e) {
            return Mono.error(e);
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .map(this::mapRollbackResult)
                .thenReturn(this);
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<RmDatabaseSession> rollback(Xid xid, Function<Option<?>, ?> optionFunc) {
        return null;
    }


    @Override
    public final Mono<RmDatabaseSession> forget(final Xid xid) {
        // mysql doesn't support this
        return Mono.just(this);
    }


    @Override
    public final Publisher<RmDatabaseSession> forget(Xid xid, Function<Option<?>, ?> optionFunc) {
        return null;
    }

    @Override
    public final Publisher<Optional<Xid>> recover(final int flags) {
        final Flux<Optional<Xid>> flux;
        if (flags != TM_NO_FLAGS && ((flags & TM_START_RSCAN) | (flags & TM_END_RSCAN)) == 0) {
            flux = Flux.error(MySQLExceptions.xaInvalidFlagForRecover(flags));
        } else if ((flags & TM_START_RSCAN) == 0) {
            flux = Flux.empty();
        } else {
            flux = this.protocol.query(Stmts.stmt("XA RECOVER"), CurrentRow::asResultRow)
                    .map(this::mapRecoverResult)
            ;
        }
        return flux;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Override
    public final Publisher<Optional<Xid>> recover(int flags, Function<Option<?>, ?> optionFunc) {
        return null;
    }


    @Override
    public final boolean isSupportForget() {
        return false;
    }

    @Override
    public final int startSupportFlags() {
        return 0;
    }

    @Override
    public final int endSupportFlags() {
        return 0;
    }

    @Override
    public final int recoverSupportFlags() {
        return 0;
    }


    /**
     * @see #start(Xid, int, TransactionOption)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Nullable
    private void xidToString(final StringBuilder cmdBuilder, final @Nullable Xid xid) throws XaException {
        if (xid == null) {
            throw MySQLExceptions.xidIsNull();
        }

        final String gtrid, bqual;
        gtrid = xid.getGtrid();
        bqual = xid.getBqual();

        final byte[] gtridBytes, bqualBytes;

        if (!MySQLStrings.hasText(gtrid)) {
            throw MySQLExceptions.xaGtridNoText();
        } else if ((gtridBytes = gtrid.getBytes(StandardCharsets.UTF_8)).length > 64) {
            throw MySQLExceptions.xaGtridBeyond64Bytes();
        }

        cmdBuilder.append(" 0x")
                .append(MySQLBuffers.hexEscapesText(true, gtridBytes, gtridBytes.length));

        cmdBuilder.append(',');
        if(bqual != null){
            if ((bqualBytes = bqual.getBytes(StandardCharsets.UTF_8)).length > 64) {
                throw MySQLExceptions.xaBqualBeyond64Bytes();
            }
            cmdBuilder.append("0x")
                    .append(MySQLBuffers.hexEscapesText(true, bqualBytes, bqualBytes.length));
        }

        cmdBuilder.append(',')
                .append(Integer.toUnsignedString(xid.getFormatId()));

    }

    /**
     * @see #start(Xid, int)
     */
    private ResultStates mapStartResult(final ResultStates states) {
        if (states.inTransaction()) {
            return states;
        }
        throw new JdbdException("XA START failure,session not in XA transaction.");
    }

    /**
     * @see #prepare(Xid)
     */
    private int mapPrepareResultCode(ResultStates states) {
        return states.valueOf(Option.READ_ONLY) ? XA_RDONLY : XA_OK;
    }

    /**
     * @see #commit(Xid, boolean)
     */
    private ResultStates mapCommitResult(final ResultStates states) {
        if (states.inTransaction()) {
            throw new JdbdException("XA COMMIT failure,session still in transaction.");
        }
        return states;
    }

    /**
     * @see #commit(Xid, boolean)
     */
    private ResultStates mapRollbackResult(final ResultStates states) {
        if (states.inTransaction()) {
            throw new JdbdException("XA ROLLBACK failure,session still in transaction.");
        }
        return states;
    }

    /**
     * @see #recover(int)
     */
    private Xid mapRecoverResult(final ResultRow row) {
        final int gtridLength, bqualLength;

        gtridLength = row.getNonNull("gtrid_length", Integer.class);
        bqualLength = row.getNonNull("bqual_length", Integer.class);

        final byte[] dataBytes;
        dataBytes = row.getNonNull("data", String.class).getBytes(StandardCharsets.UTF_8);
        if (dataBytes.length != (gtridLength + bqualLength)) {
            String m;
            m = String.format("XA Recover error,data length[%s] isn't the sum of between gtrid_length[%s] and bqual_length[%s].",
                    dataBytes.length, gtridLength, bqualLength);
            throw new JdbdException(m);
        }

        final String gtrid, bqual;
        gtrid = new String(dataBytes, 0, gtridLength, StandardCharsets.UTF_8);
        if (bqualLength == 0) {
            bqual = null;
        } else {
            bqual = new String(dataBytes, gtridLength, bqualLength, StandardCharsets.UTF_8);
        }
        return XidImpl.create(gtrid, bqual, row.getNonNull("formatID", Integer.class));
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
        public Publisher<PoolRmDatabaseSession> reconnect(Duration duration) {
            return this.protocol.reconnect(duration)
                    .thenReturn(this);
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

}
