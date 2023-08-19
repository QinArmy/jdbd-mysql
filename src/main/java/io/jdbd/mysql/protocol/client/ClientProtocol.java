package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.*;
import io.jdbd.session.*;
import io.jdbd.vendor.session.JdbdTransactionStatus;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.task.PrepareTask;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
final class ClientProtocol implements MySQLProtocol {


    static ClientProtocol create(ProtocolManager manager) {
        return new ClientProtocol(manager);

    }

    private static final String COMMIT = "COMMIT";

    private static final String ROLLBACK = "ROLLBACK";

    private static final Option<Boolean> WITH_CONSISTENT_SNAPSHOT = Option.from("WITH CONSISTENT SNAPSHOT", Boolean.class);


    final TaskAdjutant adjutant;

    private final ProtocolManager manager;

    private ClientProtocol(final ProtocolManager manager) {
        this.manager = manager;
        this.adjutant = manager.adjutant();
    }


    /*################################## blow ClientCommandProtocol method ##################################*/


    @Override
    public long threadId() {
        return this.adjutant.handshake10().threadId;
    }


    @Override
    public Mono<ResultStates> update(StaticStmt stmt) {
        return ComQueryTask.update(stmt, this.adjutant);
    }

    @Override
    public <R> Flux<R> query(StaticStmt stmt, Function<CurrentRow, R> function) {
        return ComQueryTask.query(stmt, function, this.adjutant);
    }

    @Override
    public Flux<ResultStates> batchUpdate(StaticBatchStmt stmt) {
        return ComQueryTask.batchUpdate(stmt, this.adjutant);
    }

    @Override
    public BatchQuery batchQuery(StaticBatchStmt stmt) {
        return ComQueryTask.batchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult batchAsMulti(final StaticBatchStmt stmt) {
        return ComQueryTask.batchAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux batchAsFlux(final StaticBatchStmt stmt) {
        return ComQueryTask.batchAsFlux(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux executeAsFlux(final StaticMultiStmt stmt) {
        return ComQueryTask.executeAsFlux(stmt, this.adjutant);
    }

    @Override
    public Mono<ResultStates> paramUpdate(ParamStmt stmt, boolean usePrepare) {
        final Mono<ResultStates> mono;
        if (usePrepare) {
            mono = ComPreparedTask.update(stmt, this.adjutant);
        } else {
            mono = ComQueryTask.paramUpdate(stmt, this.adjutant);
        }
        return mono;
    }

    @Override
    public <R> Flux<R> paramQuery(ParamStmt stmt, boolean usePrepare, Function<CurrentRow, R> function) {
        final Flux<R> flux;
        if (usePrepare || stmt.getFetchSize() > 0) {
            flux = ComPreparedTask.query(stmt, function, this.adjutant);
        } else {
            flux = ComQueryTask.paramQuery(stmt, function, this.adjutant);
        }
        return flux;
    }


    @Override
    public Flux<ResultStates> paramBatchUpdate(ParamBatchStmt stmt, boolean usePrepare) {
        final Flux<ResultStates> flux;
        if (usePrepare) {
            flux = ComPreparedTask.batchUpdate(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.paramBatchUpdate(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public BatchQuery paramBatchQuery(ParamBatchStmt stmt, boolean usePrepare) {
        final BatchQuery batchQuery;
        if (usePrepare) {
            batchQuery = ComPreparedTask.batchQuery(stmt, this.adjutant);
        } else {
            batchQuery = ComQueryTask.paramBatchQuery(stmt, this.adjutant);
        }
        return batchQuery;
    }

    @Override
    public MultiResult paramBatchAsMulti(final ParamBatchStmt stmt, final boolean usePrepare) {
        final MultiResult result;
        if (usePrepare) {
            result = ComPreparedTask.batchAsMulti(stmt, this.adjutant);
        } else {
            result = ComQueryTask.paramBatchAsMulti(stmt, this.adjutant);
        }
        return result;
    }

    @Override
    public OrderedFlux paramBatchAsFlux(final ParamBatchStmt stmt, final boolean usePrepare) {
        final OrderedFlux flux;
        if (usePrepare) {
            flux = ComPreparedTask.batchAsFlux(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.paramBatchAsFlux(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public Flux<ResultStates> multiStmtBatchUpdate(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtBatchUpdate(stmt, this.adjutant);
    }

    @Override
    public BatchQuery multiStmtBatchQuery(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtBatchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult multiStmtAsMulti(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux multiStmtAsFlux(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtAsFlux(stmt, this.adjutant);
    }

    @Override
    public Mono<PrepareTask> prepare(String sql) {
        return ComPreparedTask.prepare(sql, this.adjutant);
    }

    @Override
    public Mono<RefCursor> declareCursor(StaticStmt stmt) {
        return null;
    }

    @Override
    public Mono<RefCursor> paramDeclareCursor(ParamStmt stmt, boolean useServerPrepare) {
        return null;
    }

    @Override
    public Mono<Void> reconnect() {
        return this.manager.reConnect();
    }

    @Override
    public boolean supportOutParameter() {
        return this.adjutant.handshake10().serverVersion.isSupportOutParameter();
    }

    @Override
    public boolean supportSavePoints() {
        return true;
    }

    @Override
    public boolean supportStmtVar() {
        return this.adjutant.handshake10().serverVersion.isSupportQueryAttr();
    }


    @Override
    public boolean inTransaction() {
        return Terminator.inTransaction(this.adjutant.serverStatus());
    }

    @Override
    public Mono<TransactionStatus> transactionStatus() {
        final MySQLServerVersion version = this.adjutant.handshake10().serverVersion;
        final StringBuilder builder = new StringBuilder(139);
        if (version.meetsMinimum(8, 0, 3)
                || (version.meetsMinimum(5, 7, 20) && !version.meetsMinimum(8, 0, 0))) {
            builder.append("SELECT @@session.transaction_isolation AS txLevel")
                    .append(",@@session.transaction_read_only AS txReadOnly");
        } else {
            builder.append("SELECT @@session.tx_isolation AS txLevel")
                    .append(",@@session.tx_read_only AS txReadOnly");
        }

        final ResultStates[] statesHolder = new ResultStates[1];

        final StaticStmt stmt;
        stmt = Stmts.stmt(builder.toString(), states -> statesHolder[0] = states);
        return ComQueryTask.query(stmt, CurrentRow::asResultRow, this.adjutant)
                .last() // must wait for last ,because statesHolder
                .map(row -> mapTxStatus(row, statesHolder[0]));
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/commit.html">START TRANSACTION Statement</a>
     */
    @Override
    public Mono<ResultStates> startTransaction(final TransactionOption option, final HandleMode mode) {

        final StringBuilder builder = new StringBuilder(50);

        final JdbdException error;
        if (this.inTransaction() && (error = handleInTransaction(mode, builder)) != null) {
            return Mono.error(error);
        }

        final Isolation isolation = option.isolation();

        if (isolation != null) {
            builder.append("SET TRANSACTION ISOLATION LEVEL ");
            if (appendIsolation(isolation, builder)) {
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
        if (Boolean.TRUE.equals(option.valueOf(WITH_CONSISTENT_SNAPSHOT))) {
            builder.append(Constants.SPACE_COMMA_SPACE)
                    .append(WITH_CONSISTENT_SNAPSHOT.name());
        }
        return Flux.from(ComQueryTask.executeAsFlux(Stmts.multiStmt(builder.toString()), this.adjutant))
                .last()
                .map(ResultStates.class::cast);
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-transaction.html">SET TRANSACTION Statement</a>
     */
    @Override
    public Mono<Void> setTransactionOption(final TransactionOption option, final HandleMode mode) {
        final StringBuilder builder = new StringBuilder(50);

        final JdbdException error;
        if (this.inTransaction() && (error = handleInTransaction(mode, builder)) != null) {
            return Mono.error(error);
        }

        final Isolation isolation = option.isolation();

        if (isolation != null) {
            builder.append("SET TRANSACTION ISOLATION LEVEL ");
            if (appendIsolation(isolation, builder)) {
                return Mono.error(MySQLExceptions.unknownIsolation(isolation));
            }
            builder.append(Constants.SPACE_COMMA_SPACE);
        }

        if (option.isReadOnly()) {
            builder.append("READ ONLY");
        } else {
            builder.append("READ WRITE");
        }
        return Flux.from(ComQueryTask.executeAsFlux(Stmts.multiStmt(builder.toString()), this.adjutant))
                .then();
    }


    @Override
    public Mono<ResultStates> commit(List<Option<?>> optionList) {
        return Flux.from(ComQueryTask.executeAsFlux(Stmts.multiStmt(COMMIT), this.adjutant))
                .last()
                .map(ResultStates.class::cast);
    }

    @Override
    public Mono<ResultStates> rollback(List<Option<?>> optionList) {
        return Flux.from(ComQueryTask.executeAsFlux(Stmts.multiStmt(ROLLBACK), this.adjutant))
                .last()
                .map(ResultStates.class::cast);
    }


    @Override
    public <T> Mono<T> close() {
        return QuitTask.quit(this.adjutant);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T valueOf(final @Nullable Option<T> option) {
        final TaskAdjutant adjutant = this.adjutant;
        if (!adjutant.isActive()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
        final int serverStatus = adjutant.serverStatus();
        final Object value;
        if (option == null) {
            value = null;
        } else if (option == Option.AUTO_COMMIT) {
            value = (serverStatus & Terminator.SERVER_STATUS_AUTOCOMMIT) != 0;
        } else if (option == Option.IN_TRANSACTION) {
            value = Terminator.inTransaction(serverStatus);
        } else if (option == Option.READ_ONLY) {
            value = (serverStatus & Terminator.SERVER_STATUS_IN_TRANS_READONLY) != 0;
        } else if (option == Option.BACKSLASH_ESCAPES) {
            value = (serverStatus & Terminator.SERVER_STATUS_NO_BACKSLASH_ESCAPES) == 0;
        } else if (option == Option.BINARY_HEX_ESCAPES) {
            value = Boolean.TRUE;
        } else if (option == Option.CLIENT_ZONE) {
            value = adjutant.sessionEnv().connZone();
        } else if (option == Option.SERVER_ZONE) {
            value = adjutant.sessionEnv().serverZone();
        } else if (option == Option.CLIENT_CHARSET) {
            value = adjutant.sessionEnv().charsetClient();
        } else if (option == Option.AUTO_RECONNECT) {
            value = adjutant.host().properties().getOrDefault(MySQLKey.AUTO_RECONNECT);
        } else {
            value = null;
        }
        return (T) value;
    }

    @Override
    public Mono<Void> reset() {
        return Mono.defer(this.manager::reset);
    }

    @Override
    public Mono<Void> ping(final int timeSeconds) {
        return PingTask.ping(timeSeconds, this.adjutant);
    }


    @Override
    public boolean supportMultiStmt() {
        return Capabilities.supportMultiStatement(this.adjutant.capability());
    }

    @Override
    public ServerVersion serverVersion() {
        return this.adjutant.handshake10().serverVersion;
    }

    @Override
    public boolean isClosed() {
        return !this.adjutant.isActive();
    }

    /*################################## blow private method ##################################*/


    /**
     * @see #startTransaction(TransactionOption, HandleMode)
     * @see #setTransactionOption(TransactionOption, HandleMode)
     */
    @Nullable
    private JdbdException handleInTransaction(final HandleMode mode, final StringBuilder builder) {
        JdbdException error = null;
        switch (mode) {
            case ERROR_IF_EXISTS:
                error = MySQLExceptions.transactionExistsRejectStart(this.threadId());
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
     * @see #transactionStatus()
     */
    private TransactionStatus mapTxStatus(final ResultRow row, final ResultStates states) {
        Objects.requireNonNull(states, "states");

        final String txLevel;
        txLevel = row.getNonNull("txLevel", String.class);

        final Isolation isolation;
        if (txLevel.equalsIgnoreCase("READ-COMMITTED")) {
            isolation = Isolation.READ_COMMITTED;
        } else if (txLevel.equalsIgnoreCase("REPEATABLE-READ")) {
            isolation = Isolation.REPEATABLE_READ;
        } else if (txLevel.equalsIgnoreCase("SERIALIZABLE")) {
            isolation = Isolation.SERIALIZABLE;
        } else if (txLevel.equalsIgnoreCase("READ-UNCOMMITTED")) {
            isolation = Isolation.READ_UNCOMMITTED;
        } else {
            final String m;
            m = String.format("transaction_isolation[%s] couldn't map to %s", txLevel, Isolation.class.getName());
            throw new JdbdException(m);
        }
        return JdbdTransactionStatus.txStatus(isolation, row.getNonNull("txReadOnly", Boolean.class), states.inTransaction());
    }


    private static boolean appendIsolation(final Isolation isolation, final StringBuilder builder) {

        boolean error = false;
        if (isolation == Isolation.READ_COMMITTED) {
            builder.append("READ COMMITTED");
        } else if (isolation == Isolation.REPEATABLE_READ) {
            builder.append("REPEATABLE READ");
        } else if (isolation == Isolation.SERIALIZABLE) {
            builder.append("SERIALIZABLE");
        } else if (isolation == Isolation.READ_UNCOMMITTED) {
            builder.append("READ UNCOMMITTED");
        } else {
            error = true;
        }
        return error;
    }


}
