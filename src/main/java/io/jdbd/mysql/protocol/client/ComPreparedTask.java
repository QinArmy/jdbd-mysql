package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.*;
import io.jdbd.session.ChunkOption;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.SessionCloseException;
import io.jdbd.statement.BindSingleStatement;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.vendor.result.JdbdWarning;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.task.PrepareTask;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This class is the implementation of MySQL Prepared protocol.
 * </p>
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see ExecuteCommandWriter
 * @see LongParameterWriter
 * @see BinaryResultSetReader
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase_ps.html">Prepared Statements</a>
 */
final class ComPreparedTask extends MySQLCommandTask implements PrepareStmtTask, PrepareTask {


    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method:
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#paramUpdate(ParamStmt, TaskAdjutant)
     */
    static Mono<ResultStates> update(final ParamStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindStatement#executeQuery()}</li>
     *     <li>{@link BindStatement#executeQuery(Function)}</li>
     *     <li>{@link BindStatement#executeQuery(Function, Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#paramQuery(ParamStmt, Function, TaskAdjutant)
     */
    static <R> Flux<R> query(final ParamStmt stmt, final Function<CurrentRow, R> function, final TaskAdjutant adjutant) {
        return MultiResults.query(function, stmt.getStatusConsumer(), sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchUpdate()} method.
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#paramBatchUpdate(ParamBatchStmt, TaskAdjutant)
     */
    static Flux<ResultStates> batchUpdate(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }

        });
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindSingleStatement#executeBatchQuery()}.
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#paramBatchUpdate(ParamBatchStmt, TaskAdjutant)
     */
    static BatchQuery batchQuery(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.batchQuery(adjutant, sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }

        });
    }


    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsMulti()} method.
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#paramBatchAsMulti(ParamBatchStmt, TaskAdjutant)
     */
    static MultiResult batchAsMulti(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsFlux()} method.
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#paramBatchAsFlux(ParamBatchStmt, TaskAdjutant)
     */
    static OrderedFlux batchAsFlux(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    /**
     * <p>
     * This method is underlying api of below methods:
     * <ul>
     *     <li>{@link DatabaseSession#prepareStatement(String)}</li>
     * </ul>
     * </p>
     *
     * @see DatabaseSession#prepareStatement(String)
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     */
    static Mono<PrepareTask> prepare(final String sql, final TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                final MySQLPrepareStmt stmt = new MySQLPrepareStmt(sql);
                final ResultSink resultSink = new PrepareSink(sink);
                ComPreparedTask task = new ComPreparedTask(stmt, resultSink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    private static final Logger LOG = LoggerFactory.getLogger(ComPreparedTask.class);

//    /**
//     * {@code enum_resultset_metadata} No metadata will be sent.
//     *
//     * @see #RESULTSET_METADATA_FULL
//     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#aba06d1157f6dee3f20537154103c91a1">enum_resultset_metadata</a>
//     */
//    private static final byte RESULTSET_METADATA_NONE = 0;
    /**
     * {@code enum_resultset_metadata} The server will send all metadata.
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#aba06d1157f6dee3f20537154103c91a1">enum_resultset_metadata</a>
     */
    private static final byte RESULTSET_METADATA_FULL = 1;

    private final ParamSingleStmt stmt;

    private final CommandWriter commandWriter;

    private int statementId;

    private Warning warning;

    private Phase phase = Phase.PREPARED;

    private MySQLColumnMeta[] paramMetas;

    private ResultRowMeta rowMeta;

    /**
     * @see #nextGroupReset()
     * @see #readResetResponse(ByteBuf, Consumer)
     */
    private boolean nextGroupNeedReset;

    private int batchIndex = 0;

    /**
     * @see #update(ParamStmt, TaskAdjutant)
     */
    private ComPreparedTask(final ParamSingleStmt stmt, final ResultSink sink, final TaskAdjutant adjutant) {
        super(adjutant, sink);

        assert (this.capability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) == 0; // currently, dont' support cache.

        this.stmt = stmt;
        this.commandWriter = ExecuteCommandWriter.create(this);
    }

    @Override
    public ParamSingleStmt getStmt() {
        return this.stmt;
    }

    @Override
    public void resetSequenceId() {
        updateSequenceId(-1);
    }

    @Override
    public int getStatementId() {
        if (this.paramMetas == null) {
            throw new IllegalStateException("before prepare");
        }
        return this.statementId;
    }

    @Override
    public MySQLColumnMeta[] getParameterMetas() {
        return Objects.requireNonNull(this.paramMetas, "this.parameterMetas");
    }

    @Override
    public void handleExecuteMessageError() {
        //TODO
    }

    @Override
    public boolean isSupportFetch() {
        return this.rowMeta != null && this.stmt.getFetchSize() > 0;
    }

    @Override
    public void nextGroupReset() {
        this.nextGroupNeedReset = true;
    }


    /*################################## blow PrepareStmtTask method ##################################*/

    /**
     * @see PrepareTask#executeUpdate(ParamStmt)
     */
    @Override
    public Mono<ResultStates> executeUpdate(final ParamStmt stmt) {
        return MultiResults.update(sink -> executeAfterBinding(sink, stmt));
    }


    @Override
    public <R> Flux<R> executeQuery(final ParamStmt stmt, Function<CurrentRow, R> function) {
        return MultiResults.query(function, stmt.getStatusConsumer(), sink -> executeAfterBinding(sink, stmt));
    }

    /**
     * @see PrepareTask#executeBatchUpdate(ParamBatchStmt)
     */
    @Override
    public Flux<ResultStates> executeBatchUpdate(final ParamBatchStmt stmt) {
        return MultiResults.batchUpdate(sink -> executeAfterBinding(sink, stmt));
    }


    @Override
    public BatchQuery executeBatchQuery(ParamBatchStmt stmt) {
        return MultiResults.batchQuery(this.adjutant, sink -> executeAfterBinding(sink, stmt));
    }

    /**
     * @see PrepareTask#executeBatchAsMulti(ParamBatchStmt)
     */
    @Override
    public MultiResult executeBatchAsMulti(ParamBatchStmt stmt) {
        return MultiResults.asMulti(this.adjutant, sink -> executeAfterBinding(sink, stmt));
    }

    /**
     * @see PrepareTask#executeBatchAsFlux(ParamBatchStmt)
     */
    @Override
    public OrderedFlux executeBatchAsFlux(ParamBatchStmt stmt) {
        return MultiResults.asFlux(sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public List<MySQLType> getParamTypes() {
        final MySQLColumnMeta[] paramMetas = this.paramMetas;
        if (paramMetas == null) {
            throw new IllegalStateException("this.paramMetas is null");
        }
        final List<MySQLType> paramTypeList;
        switch (paramMetas.length) {
            case 0: {
                paramTypeList = Collections.emptyList();
            }
            break;
            case 1: {
                paramTypeList = Collections.singletonList(paramMetas[0].sqlType);
            }
            break;
            default: {
                final List<MySQLType> list = MySQLCollections.arrayList(paramMetas.length);
                for (MySQLColumnMeta paramMeta : paramMetas) {
                    list.add(paramMeta.sqlType);
                }
                paramTypeList = Collections.unmodifiableList(list);
            }
        }
        return paramTypeList;
    }

    @Nullable
    @Override
    public ResultRowMeta getRowMeta() {
        return this.rowMeta;
    }

    @Override
    public void closeOnBindError(final Throwable error) {
        if (this.adjutant.inEventLoop()) {
            closeOnBindErrorInEventLoop(error);
        } else {
            this.adjutant.execute(() -> closeOnBindErrorInEventLoop(error));
        }
    }

    @Override
    public String getSql() {
        return this.stmt.getSql();
    }

    @Override
    public Mono<Void> abandonBind() {
        return Mono.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                abandonBindInEventLoop(sink);
            } else {
                this.adjutant.execute(() -> abandonBindInEventLoop(sink));
            }
        });
    }

    @Nullable
    @Override
    public Warning getWarning() {
        return this.warning;
    }

    /*################################## blow CommunicationTask protected  method ##################################*/

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html">Protocol::COM_STMT_PREPARE</a>
     */
    @Override
    protected Publisher<ByteBuf> start() {
        Publisher<ByteBuf> publisher;
        try {
            publisher = createPreparePacket();
            this.phase = Phase.READ_PREPARE_RESPONSE;

            if (LOG.isTraceEnabled()) {
                LOG.trace(" {} prepare with stmt[{}]", this, this.stmt.getClass().getSimpleName());
            }
        } catch (Throwable e) {
            publisher = null;
            this.phase = Phase.ERROR_ON_START;
            addError(e);
        }
        return publisher;
    }


    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        if (this.phase == Phase.ERROR_ON_START) {
            publishError(this.sink::error);
            return true;
        }
        boolean taskEnd = false, continueDecode = Packets.hasOnePacket(cumulateBuffer);
        while (continueDecode) {
            switch (this.phase) {
                case READ_PREPARE_RESPONSE: {
                    if (canReadPrepareResponse(cumulateBuffer)) {
                        // possibly PreparedStatement bind occur error
                        taskEnd = readPrepareResponse(cumulateBuffer, serverStatusConsumer)
                                || this.phase == Phase.ERROR_ON_BINDING
                                || this.phase == Phase.ABANDON_BINDING;
                    }
                    continueDecode = false;
                }
                break;
                case READ_EXECUTE_RESPONSE: {
                    // maybe modify this.phase
                    taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
                    continueDecode = false;
                }
                break;
                case READ_RESULT_SET: {
                    taskEnd = readResultSet(cumulateBuffer, serverStatusConsumer);
                    continueDecode = false;
                }
                break;
                case READ_RESET_RESPONSE: {
                    if (readResetResponse(cumulateBuffer, serverStatusConsumer)) {
                        taskEnd = true;
                    } else {
                        this.phase = Phase.EXECUTE;
                        taskEnd = executeNextGroup(); // execute command
                    }
                    continueDecode = false;
                }
                break;
                case READ_FETCH_RESPONSE: {
                    if (readFetchResponse(cumulateBuffer)) {
                        taskEnd = true;
                        continueDecode = false;
                    } else {
                        this.phase = Phase.READ_RESULT_SET;
                        continueDecode = Packets.hasOnePacket(cumulateBuffer);
                    }
                }
                break;
                default:
                    throw MySQLExceptions.unexpectedEnum(this.phase);
            }
        }
        if (taskEnd) {
            if (this.phase != Phase.READ_PREPARE_RESPONSE) {
                this.phase = Phase.END;
                this.packetPublisher = Mono.just(createCloseStatementPacket());
            }
            if (hasError()) {
                publishError(this.sink::error);
            } else {
                this.sink.complete();
            }
        }
        return taskEnd;
    }


    @Override
    protected Action onError(Throwable e) {
        final Action action;
        switch (this.phase) {
            case PREPARED:
                action = Action.TASK_END;
                break;
            case END: {
                throw new IllegalStateException("CLOSE_STMT command send error.", e);
            }
            default: {
                addError(MySQLExceptions.wrap(e));
                this.packetPublisher = Mono.just(createCloseStatementPacket());
                action = Action.MORE_SEND_AND_END;
            }

        }
        return action;
    }

    @Override
    protected void onChannelClose() {
        if (this.phase != Phase.END) {
            this.sink.error(new SessionCloseException("Database session unexpected close."));
        }
    }

    @Override
    protected boolean skipPacketsOnError(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        log.debug("error ");
        return super.skipPacketsOnError(cumulateBuffer, serverStatusConsumer);
    }

    /*################################## blow packet template method ##################################*/

    @Override
    void handleReadResultSetEnd() {
        this.phase = Phase.READ_EXECUTE_RESPONSE;
    }

    @Override
    ResultSetReader createResultSetReader() {
        return BinaryResultSetReader.create(this);
    }

    @Override
    boolean hasMoreGroup() {
        final ParamSingleStmt stmt = getActualStmt();
        final boolean moreGroup;
        if (stmt instanceof ParamBatchStmt) {
            moreGroup = this.batchIndex < ((ParamBatchStmt) stmt).getGroupList().size();
            if (moreGroup) {
                this.phase = Phase.EXECUTE;
            }
        } else {
            moreGroup = false;
        }
        return moreGroup;
    }


    /**
     * @return true : task end.
     * @see #handleReadPrepareComplete()
     */
    boolean executeNextGroup() {
        assertPhase(Phase.EXECUTE);

        if (this.nextGroupNeedReset) {
            this.packetPublisher = Mono.just(createResetPacket());
            this.phase = Phase.READ_RESET_RESPONSE;
            return false;
        }

        final ParamSingleStmt stmt = this.stmt;
        final ParamSingleStmt actualStmt;
        if (stmt instanceof PrepareStmt) {
            actualStmt = ((PrepareStmt) stmt).getStmt();
        } else {
            actualStmt = stmt;
        }

        final int batchIndex;
        if (actualStmt instanceof ParamStmt) {
            if (this.batchIndex++ != 0) {
                throw new IllegalStateException(String.format("%s duplication execution.", ParamStmt.class.getName()));
            }
            batchIndex = -1;
        } else {
            batchIndex = this.batchIndex++;
        }
        boolean taskEnd = false;
        try {
            this.packetPublisher = this.commandWriter.writeCommand(batchIndex);
            this.phase = Phase.READ_EXECUTE_RESPONSE;
        } catch (Throwable e) {
            taskEnd = true;
            addError(e);
        }
        return taskEnd;
    }

    /**
     * @return true: task end.
     * @see #readResultSet(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_fetch.html">Protocol::COM_STMT_FETCH</a>
     */
    @Override
    boolean executeNextFetch() {
        assertPhase(Phase.READ_EXECUTE_RESPONSE);

        final int fetchSize = getActualStmt().getFetchSize();
        final boolean taskEnd;
        if (fetchSize > 0 && this.rowMeta != null) {
            final ByteBuf packet = this.adjutant.allocator().buffer(13);

            Packets.writeInt3(packet, 9);
            packet.writeByte(nextSequenceId());

            packet.writeByte(Packets.COM_STMT_FETCH);
            Packets.writeInt4(packet, this.statementId);
            Packets.writeInt4(packet, fetchSize);

            this.packetPublisher = Mono.just(packet);
            taskEnd = false;
        } else {
            taskEnd = true;
            // here bug or MySQL server status error.
            addError(new IllegalStateException("no more fetch."));
        }
        return taskEnd;
    }

    /*################################## blow private method ##################################*/

    private Publisher<ByteBuf> createPreparePacket() {
        final byte[] sqlBytes;
        sqlBytes = this.stmt.getSql().getBytes(this.adjutant.charsetClient());

        final ByteBuf packet;
        packet = this.adjutant.allocator().buffer(Packets.HEADER_SIZE + 1 + sqlBytes.length);
        packet.writeZero(Packets.HEADER_SIZE);  // placeholder of header

        packet.writeByte(Packets.COM_STMT_PREPARE);
        packet.writeBytes(sqlBytes);
        return Packets.createPacketPublisher(packet, this::nextSequenceId, this.adjutant);
    }

    private ParamSingleStmt getActualStmt() {
        ParamSingleStmt stmt = this.stmt;
        if (stmt instanceof PrepareStmt) {
            stmt = ((PrepareStmt) stmt).getStmt();
        }
        return stmt;
    }


    /**
     * @see #executeUpdate(ParamStmt)
     * @see #executeQuery(ParamStmt, Function)
     * @see #executeBatchUpdate(ParamBatchStmt)
     * @see #executeBatchAsMulti(ParamBatchStmt)
     * @see #executeBatchAsFlux(ParamBatchStmt)
     */
    private void executeAfterBinding(final ResultSink sink, final ParamSingleStmt stmt) {
        if (this.adjutant.inEventLoop()) {
            executeAfterBindingInEventLoop(sink, stmt);
        } else {
            this.adjutant.execute(() -> executeAfterBindingInEventLoop(sink, stmt));
        }
    }

    /**
     * @see #executeAfterBinding(ResultSink, ParamSingleStmt)
     */
    private void executeAfterBindingInEventLoop(final ResultSink sink, final ParamSingleStmt stmt) {
        switch (this.phase) {
            case WAIT_FOR_BINDING: {
                try {
                    ((MySQLPrepareStmt) this.stmt).setStmt(stmt);
                    ((PrepareSink) this.sink).setSink(sink);
                    this.phase = Phase.EXECUTE;
                    if (executeNextGroup()) {
                        endTaskForErrorOnWaitForBinding();
                    } else {
                        this.phase = Phase.READ_EXECUTE_RESPONSE;
                    }
                } catch (Throwable e) {
                    // here bug
                    this.phase = Phase.ERROR_ON_BINDING;
                    endTaskForErrorOnWaitForBinding();
                    sink.error(e);
                }
            }
            break;
            case END: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} have ended,but reuse {}", this, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
            break;
            default: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} is executing,but reuse {}", this, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
        }

    }

    /**
     * @see #abandonBind()
     */
    private void abandonBindInEventLoop(final MonoSink<Void> sink) {
        // error can't be emitted to sink ,because not actual sink now.
        switch (this.phase) {
            case WAIT_FOR_BINDING: {
                this.phase = Phase.ABANDON_BINDING;
                endTaskForErrorOnWaitForBinding();
                sink.success();
            }
            break;
            case END: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} have ended,but reuse {}", this, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
            break;
            case ERROR_ON_BINDING:
            case ABANDON_BINDING: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("this.phase is {},but reuse {}", this.phase, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
            break;
            default: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} is executing,but reuse {}", this, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
        } // switch
    }

    /**
     * @see #closeOnBindError(Throwable)
     */
    private void closeOnBindErrorInEventLoop(final Throwable error) {
        // error can't be emitted to sink ,because not actual sink now.
        switch (this.phase) {
            case WAIT_FOR_BINDING: {
                this.phase = Phase.ERROR_ON_BINDING;
                endTaskForErrorOnWaitForBinding();
            }
            break;
            case END: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} have ended,but reuse {}", this, PreparedStatement.class.getName(), error);
                }
            }
            break;
            case ERROR_ON_BINDING:
            case ABANDON_BINDING: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("this.phase is {},but reuse {}", this.phase, PreparedStatement.class.getName(), error);
                }
            }
            break;
            default: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} is executing,but reuse {}", this, PreparedStatement.class.getName(), error);
                }
            }
        } // switch

    }

    /**
     * @see #closeOnBindErrorInEventLoop(Throwable)
     */
    private void endTaskForErrorOnWaitForBinding() {
        if (!this.inDecodeMethod()) {
            this.packetPublisher = Mono.just(createCloseStatementPacket());
            this.sendPacketSignal(true)
                    .doOnSuccess(v -> this.phase = Phase.END)
                    .subscribe();
        }

    }


    /**
     * @return false : need more cumulate
     * @see #readPrepareResponse(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response">COM_STMT_PREPARE Response</a>
     */
    private boolean canReadPrepareResponse(final ByteBuf cumulateBuffer) {
        final int originalReaderIndex = cumulateBuffer.readerIndex();

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        cumulateBuffer.readByte();// skip sequenceId byte.

        final int payloadIndex = cumulateBuffer.readerIndex();
        final boolean canRead;
        final int headFlag = Packets.readInt1AsInt(cumulateBuffer);
        switch (headFlag) {
            case ErrorPacket.ERROR_HEADER:
                canRead = true;
                break;
            case 0: {
                cumulateBuffer.skipBytes(4); //statementId
                final int numColumns, numParams, capability;
                numColumns = Packets.readInt2AsInt(cumulateBuffer);
                numParams = Packets.readInt2AsInt(cumulateBuffer);
                capability = this.capability;

                int packetNumber = 0;
                if (payloadLength > 12) {
                    cumulateBuffer.skipBytes(2); // skip warning_count
                    if ((capability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) == 0
                            || cumulateBuffer.readByte() == RESULTSET_METADATA_FULL) {
                        packetNumber = numColumns + numParams;
                    }
                } else if ((capability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) == 0) {
                    packetNumber = numColumns + numParams;
                }

                if (packetNumber > 0 && (capability & Capabilities.CLIENT_DEPRECATE_EOF) == 0) {
                    packetNumber += 2;
                }

                cumulateBuffer.readerIndex(payloadIndex + payloadLength); // to next packet,avoid tailor filler.

                canRead = Packets.hasPacketNumber(cumulateBuffer, packetNumber);
            }
            break;
            default: {
                cumulateBuffer.readerIndex(originalReaderIndex);
                String m = String.format("Server send COM_STMT_PREPARE Response error. header[%s]", headFlag);
                throw MySQLExceptions.createFatalIoException(m, null);
            }

        }
        cumulateBuffer.readerIndex(originalReaderIndex);
        return canRead;
    }


    /**
     * @return true: prepare error,task end.
     * @see #decode(ByteBuf, Consumer)
     * @see #canReadPrepareResponse(ByteBuf)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response">COM_STMT_PREPARE Response</a>
     */
    private boolean readPrepareResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_PREPARE_RESPONSE);

        final int headFlag = Packets.getHeaderFlag(cumulateBuffer);
        final boolean taskEnd;
        switch (headFlag) {
            case ErrorPacket.ERROR_HEADER: {
                readErrorPacket(cumulateBuffer);
                taskEnd = true;
            }
            break;
            case 0: {
                final int payloadLength;
                payloadLength = Packets.readInt3(cumulateBuffer);
                updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));

                final int payloadIndex = cumulateBuffer.readerIndex();

                cumulateBuffer.readByte();//1. skip status
                this.statementId = Packets.readInt4(cumulateBuffer);//2. statement_id

                final int numColumns, numParams, capability = this.capability;
                numColumns = Packets.readInt2AsInt(cumulateBuffer);//3. num_columns
                numParams = Packets.readInt2AsInt(cumulateBuffer);//4. num_params
                cumulateBuffer.readByte(); //5. skip filler

                final boolean hasMeta;
                if (payloadLength > 12) {
                    final int warnings = Packets.readInt2AsInt(cumulateBuffer);//6. warning_count
                    if (warnings > 0) {
                        this.warning = JdbdWarning.create(String.format("produce %s warnings", warnings));
                    }

                    hasMeta = (capability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) == 0
                            || cumulateBuffer.readByte() == RESULTSET_METADATA_FULL;
                } else {
                    hasMeta = (capability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) == 0;
                }
                cumulateBuffer.readerIndex(payloadIndex + payloadLength); //avoid tailor filler.


                if (hasMeta) {
                    // below read param and column meta data
                    final boolean readEof = (capability & Capabilities.CLIENT_DEPRECATE_EOF) == 0;
                    this.paramMetas = MySQLColumnMeta.readMetas(cumulateBuffer, numParams, this);
                    if (readEof) {
                        readEofOfMeta(cumulateBuffer, serverStatusConsumer);
                    }
                    this.rowMeta = MySQLRowMeta.readForPrepare(cumulateBuffer, numColumns, this);
                    if (readEof) {
                        readEofOfMeta(cumulateBuffer, serverStatusConsumer);
                    }
                } else {
                    this.paramMetas = MySQLColumnMeta.EMPTY;
                    this.rowMeta = MySQLRowMeta.EMPTY;
                }
                taskEnd = handleReadPrepareComplete();
            }
            break;
            default: {
                String m = String.format("Server send COM_STMT_PREPARE Response error. header[%s]", headFlag);
                throw MySQLExceptions.createFatalIoException(m, null);
            }
        }
        return taskEnd;
    }

    private void readEofOfMeta(final ByteBuf cumulateBuffer, final Consumer<Object> consumer) {
        final int payloadLength;
        payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
        consumer.accept(EofPacket.readCumulate(cumulateBuffer, payloadLength, this.capability));
    }

    /**
     * <p>
     * this method will update {@link #phase}.
     * </p>
     *
     * @return true : task end.
     * @see #readPrepareResponse(ByteBuf, Consumer)
     */
    private boolean handleReadPrepareComplete() {
        assertPhase(Phase.READ_PREPARE_RESPONSE);
        if (this.paramMetas == null) {
            throw new IllegalStateException("this.paramMetas is null.");
        }
        final ParamSingleStmt stmt = this.stmt;
        final boolean taskEnd;
        if (stmt instanceof PrepareStmt) {
            final PrepareSink sink = (PrepareSink) this.sink;
            this.phase = Phase.WAIT_FOR_BINDING; // first modify phase to WAIT_FOR_BINDING
            sink.statementSink.success(this);
            taskEnd = false;
        } else {
            this.phase = Phase.EXECUTE;
            taskEnd = executeNextGroup();
        }
        return taskEnd;
    }


    /**
     * @see #decode(ByteBuf, Consumer)
     * @see #onError(Throwable)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html">Protocol::COM_STMT_CLOSE</a>
     */
    private ByteBuf createCloseStatementPacket() {
        ByteBuf packet = this.adjutant.allocator().buffer(9);

        Packets.writeInt3(packet, 5);
        packet.writeByte(0);// use 0 sequence_id

        packet.writeByte(Packets.COM_STMT_CLOSE);
        Packets.writeInt4(packet, this.statementId);
        return packet;
    }


    /**
     * <p>
     * modify {@link #phase}
     * </p>
     *
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     * @see #executeNextGroup()
     */
    private boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_EXECUTE_RESPONSE);

        final boolean traceEnabled = LOG.isTraceEnabled();
        if (traceEnabled) {
            LOG.trace("{} read execute response", this);
        }
        final int header = Packets.getHeaderFlag(cumulateBuffer);
        final boolean taskEnd;
        switch (header) {
            case ErrorPacket.ERROR_HEADER: {
                readErrorPacket(cumulateBuffer);
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                taskEnd = readUpdateResult(cumulateBuffer, serverStatusConsumer);
            }
            break;
            default: {
                this.phase = Phase.READ_RESULT_SET;
                taskEnd = readResultSet(cumulateBuffer, serverStatusConsumer);
            }
        }
        return taskEnd;
    }


    /**
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean readFetchResponse(final ByteBuf cumulateBuffer) {
        final int flag = Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex() + Packets.HEADER_SIZE);
        final boolean taskEnd;
        if (flag == ErrorPacket.ERROR_HEADER) {
            readErrorPacket(cumulateBuffer);
            taskEnd = true;
        } else {
            taskEnd = false;
        }
        return taskEnd;
    }

    /**
     * @return true : reset occur error,task end.
     * @see #decode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_reset.html">Protocol::COM_STMT_RESET</a>
     */
    private boolean readResetResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final int flag = Packets.getHeaderFlag(cumulateBuffer);
        final boolean taskEnd;
        switch (flag) {
            case ErrorPacket.ERROR_HEADER: {
                readErrorPacket(cumulateBuffer);
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                final int payloadLength = Packets.readInt3(cumulateBuffer);
                updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
                final OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.capability);
                serverStatusConsumer.accept(ok);
                this.nextGroupNeedReset = false;
                taskEnd = false;
            }
            break;
            default: {
                String m = String.format("COM_STMT_RESET response error,flag[%s].", flag);
                throw MySQLExceptions.createFatalIoException(m, null);
            }
        }
        return taskEnd;
    }


    /**
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_reset.html">Protocol::COM_STMT_RESET</a>
     */
    private ByteBuf createResetPacket() {
        if (this.paramMetas == null) {
            throw new IllegalStateException("before prepare");
        }
        if (!this.nextGroupNeedReset) {
            throw new IllegalStateException("don't need reset.");
        }
        ByteBuf packet = this.adjutant.allocator().buffer(9);

        Packets.writeInt3(packet, 5);
        packet.writeByte(0);// use 0 sequence id

        packet.writeByte(Packets.COM_STMT_RESET);
        Packets.writeInt4(packet, this.statementId);
        return packet;
    }


    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s.", expectedPhase));
        }
    }

    /*################################## blow private static method ##################################*/



    /*################################## blow private static inner class ##################################*/

    private static final class MySQLPrepareStmt implements PrepareStmt {

        private final String sql;

        private ParamSingleStmt stmt;

        private MySQLPrepareStmt(String sql) {
            this.sql = sql;
        }

        private void setStmt(ParamSingleStmt stmt) {
            if (this.stmt != null) {
                throw new IllegalStateException("this.stmt is non-null.");
            }
            if (!this.sql.equals(stmt.getSql())) {
                throw new IllegalArgumentException("stmt sql and original sql not match.");
            }
            this.stmt = stmt;
        }

        @Override
        public ParamSingleStmt getStmt() {
            final ParamSingleStmt stmt = this.stmt;
            if (stmt == null) {
                throw new IllegalStateException("this.stmt is null.");
            }
            return stmt;
        }

        @Override
        public List<NamedValue> getStmtVarList() {
            return this.getStmt().getStmtVarList();
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public int getTimeout() {
            return getStmt().getTimeout();
        }

        @Override
        public int getFetchSize() {
            return getStmt().getFetchSize();
        }

        @Override
        public Function<ChunkOption, Publisher<byte[]>> getImportFunction() {
            return getStmt().getImportFunction();
        }

        @Override
        public Function<ChunkOption, Subscriber<byte[]>> getExportFunction() {
            return getStmt().getExportFunction();
        }

    }

    private static final class PrepareSink implements ResultSink {

        private final MonoSink<PrepareTask> statementSink;

        private ResultSink sink;

        private PrepareSink(MonoSink<PrepareTask> statementSink) {
            this.statementSink = statementSink;
        }

        private void setSink(ResultSink sink) {
            if (this.sink != null) {
                throw new IllegalStateException("this.sink is non-null.");
            }
            this.sink = sink;
        }

        @Override
        public void error(Throwable e) {
            final ResultSink sink = this.sink;
            if (sink == null) {
                this.statementSink.error(e);
            } else {
                sink.error(e);
            }
        }

        @Override
        public void complete() {
            final ResultSink sink = this.sink;
            if (sink != null) {// if null ,possibly error on  binding
                sink.complete();
            }
        }

        @Override
        public boolean isCancelled() {
            final ResultSink sink = this.sink;
            return sink != null && sink.isCancelled();
        }

        @Override
        public void next(ResultItem result) {
            final ResultSink sink = this.sink;
            if (sink == null) {
                throw new IllegalStateException("this.sink is null");
            }
            sink.next(result);
        }

    }


    enum Phase {
        ERROR_ON_START,
        PREPARED,

        READ_PREPARE_RESPONSE,

        WAIT_FOR_BINDING,

        EXECUTE,
        READ_EXECUTE_RESPONSE,
        READ_RESULT_SET,

        RESET_STMT,
        READ_RESET_RESPONSE,

        FETCH_STMT,
        READ_FETCH_RESPONSE,

        ERROR_ON_BINDING,
        ABANDON_BINDING,

        END
    }


}
