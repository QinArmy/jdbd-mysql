package io.jdbd.mysql.session;


import io.jdbd.JdbdException;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLBuffers;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.vendor.session.XidImpl;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

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
    public final Publisher<RmDatabaseSession> setTransactionOption(TransactionOption option) {
        return this.setTransactionOption(option, HandleMode.ERROR_IF_EXISTS);
    }

    @Override
    public final Publisher<RmDatabaseSession> setTransactionOption(TransactionOption option, HandleMode mode) {
        return this.protocol.setTransactionOption(option, mode)
                .thenReturn(this);
    }

    @Override
    public final Mono<RmDatabaseSession> start(final Xid xid, final int flags) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA START");
        try {
            appendXid(builder, xid);
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
                .map(this::mapStartResult)
                .thenReturn(this);
    }


    @Override
    public final Mono<RmDatabaseSession> end(final Xid xid, final int flags) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA END");
        try {
            appendXid(builder, xid);
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
            appendXid(builder, xid);
        } catch (Throwable e) {
            return Mono.error(e);
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .map(this::mapPrepareResultCode);

    }


    @Override
    public final Mono<RmDatabaseSession> commit(Xid xid, final boolean onePhase) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA COMMIT");
        try {
            appendXid(builder, xid);
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

    @Override
    public final Mono<RmDatabaseSession> rollback(final Xid xid) {
        final StringBuilder builder = new StringBuilder(140);
        builder.append("XA ROLLBACK");
        try {
            appendXid(builder, xid);
        } catch (Throwable e) {
            return Mono.error(e);
        }
        return this.protocol.update(Stmts.stmt(builder.toString()))
                .map(this::mapRollbackResult)
                .thenReturn(this);
    }

    @Override
    public final Mono<RmDatabaseSession> forget(final Xid xid) {
        // mysql doesn't support this
        return Mono.just(this);
    }

    @Override
    public final Flux<Xid> recover(final int flags) {
        final Flux<Xid> flux;
        if (flags != TM_NO_FLAGS && ((flags & TM_START_RSCAN) | (flags & TM_END_RSCAN)) == 0) {
            flux = Flux.error(MySQLExceptions.xaInvalidFlagForRecover(flags));
        } else if ((flags & TM_START_RSCAN) == 0) {
            flux = Flux.empty();
        } else {
            flux = this.protocol.query(Stmts.stmt("XA RECOVER"), CurrentRow::asResultRow)
                    .map(this::mapRecoverResult);
        }
        return flux;
    }


    private void appendXid(final StringBuilder cmdBuilder, final Xid xid) throws JdbdException {
        Objects.requireNonNull(xid, "xid");

        final String gtrid = xid.getGtrid();
        if (MySQLStrings.hasText(gtrid)) {
            final byte[] bytes;
            bytes = gtrid.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > 64) {
                throw MySQLExceptions.xaGtridBeyond64Bytes();
            }
            cmdBuilder.append(" 0x'")
                    .append(MySQLBuffers.hexEscapesText(true, bytes, bytes.length))
                    .append("'");
        } else {
            throw MySQLExceptions.xaGtridNoText();
        }

        final String bqual = xid.getBqual();
        if (bqual != null) {
            final byte[] bytes;
            bytes = bqual.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > 64) {
                throw MySQLExceptions.xaBqualBeyond64Bytes();
            }
            cmdBuilder.append(",0x'")
                    .append(MySQLBuffers.hexEscapesText(true, bytes, bytes.length))
                    .append("'");
        }
        cmdBuilder.append(",")
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
        public Publisher<PoolRmDatabaseSession> reconnect() {
            return this.protocol.reconnect()
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
