package io.jdbd.mysql.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.UnrecognizedCollationException;
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
import java.util.Set;
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
     * @see ComQueryTask#paramQuery(ParamStmt, Function, Consumer, TaskAdjutant)
     */
    static <R> Flux<R> query(final ParamStmt stmt, final Function<CurrentRow, R> function,
                             final Consumer<ResultStates> consumer, final TaskAdjutant adjutant) {
        return MultiResults.query(function, consumer, sink -> {
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
     * This method is one of underlying api of {@link BindSingleStatement#executeAsFlux()}.
     * </p>
     *
     * @see io.jdbd.mysql.protocol.MySQLProtocol#paramAsFlux(ParamStmt, boolean)
     */
    static OrderedFlux asFlux(final ParamStmt stmt, final TaskAdjutant adjutant) {
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
    static QueryResults batchQuery(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
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

    private TaskPhase taskPhase;

    private BindPhase bindPhase = BindPhase.NONE;

    private MySQLColumnMeta[] paramMetas;

    private MySQLRowMeta rowMeta;

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
        this.taskPhase = TaskPhase.PREPARED;
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
    public Mono<ByteBuf> handleExecuteMessageError(final Throwable error) {
        final Mono<ByteBuf> mono;
        if (this.adjutant.inEventLoop()) {
            handleExecuteMessageErrorInEventLoop(error);
            mono = Mono.empty();
        } else {
            mono = Mono.create(sink -> this.adjutant.execute(() -> handleExecuteMessageErrorInEventLoop(error)));
        }
        return mono;
    }


    @Override
    public boolean isSupportFetch() {
        final MySQLRowMeta rowMeta = this.rowMeta;
        return rowMeta != null && rowMeta.columnMetaArray.length > 0 && this.stmt.getFetchSize() > 0;
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
    public <R> Flux<R> executeQuery(final ParamStmt stmt, Function<CurrentRow, R> function, Consumer<ResultStates> consumer) {
        return MultiResults.query(function, consumer, sink -> executeAfterBinding(sink, stmt));
    }

    /**
     * @see PrepareTask#executeBatchUpdate(ParamBatchStmt)
     */
    @Override
    public Flux<ResultStates> executeBatchUpdate(final ParamBatchStmt stmt) {
        return MultiResults.batchUpdate(sink -> executeAfterBinding(sink, stmt));
    }


    @Override
    public QueryResults executeBatchQuery(ParamBatchStmt stmt) {
        return MultiResults.batchQuery(this.adjutant, sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public OrderedFlux executeAsFlux(ParamStmt stmt) {
        return MultiResults.asFlux(sink -> executeAfterBinding(sink, stmt));
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
    public List<? extends DataType> getParamTypes() {
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

    @Override
    public void suspendTask() {
        throw new UnsupportedOperationException("mysql don't need ,and don't need");
    }

    @Override
    public ResultRowMeta getRowMeta() {
        final ResultRowMeta meta = this.rowMeta;
        if (meta == null) {
            throw new IllegalStateException("meta is null");
        }
        return meta;
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
    public void abandonBind() {
        if (this.adjutant.inEventLoop()) {
            abandonBindInEventLoop();
        } else {
            this.adjutant.execute(this::abandonBindInEventLoop);
        }
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
        if (startTimeoutTaskIfNeed()) {
            this.taskPhase = TaskPhase.ERROR_ON_START;
            return null;
        }

        Publisher<ByteBuf> publisher;
        switch (this.taskPhase) {
            case PREPARED: {
                try {
                    publisher = createPreparePacket();
                    this.taskPhase = TaskPhase.READ_PREPARE_RESPONSE;

                    if (LOG.isTraceEnabled()) {
                        LOG.trace(" {} prepare with stmt[{}]", this, this.stmt.getClass().getSimpleName());
                    }
                } catch (Throwable e) {
                    publisher = null;
                    this.taskPhase = TaskPhase.ERROR_ON_START;
                    addError(e);
                }
            }
            break;
            case RESUME: {
                publisher = this.packetPublisher;
                switch (this.bindPhase) {
                    case BIND_COMPLETE: {
                        if (publisher == null) {
                            // no bug,never here
                            this.taskPhase = TaskPhase.ERROR_ON_START;
                            addError(new IllegalStateException("resume for executing,but packetPublisher is null"));
                        } else {
                            this.taskPhase = TaskPhase.READ_EXECUTE_RESPONSE;
                        }
                    }
                    break;
                    case ERROR_ON_BIND:
                    case ABANDON_BIND: {
                        // here , close statement
                        LOG.debug("resume for closing statement {}", this.statementId);
                        if (publisher != null) {
                            addError(new IllegalStateException("resume for closing statement,but packetPublisher is non-null"));
                        }
                        this.taskPhase = TaskPhase.END;
                    }
                    break;
                    default: { // no bug,never here
                        publisher = null;
                        this.taskPhase = TaskPhase.ERROR_ON_START;
                        addError(MySQLExceptions.unexpectedEnum(this.bindPhase));
                    }
                } //inner switch
            }
            break;
            case NONE: // use cache
            default: {
                // no bug,never here
                publisher = null;
                addError(MySQLExceptions.unexpectedEnum(this.taskPhase));
                this.taskPhase = TaskPhase.ERROR_ON_START;
            }

        } // outer switch

        this.packetPublisher = null;
        return publisher;
    }


    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        if (this.taskPhase == TaskPhase.ERROR_ON_START) {
            publishError(this.sink::error);
            return true;
        }
        boolean taskEnd = this.taskPhase == TaskPhase.END, continueDecode = Packets.hasOnePacket(cumulateBuffer);
        while (continueDecode) {
            switch (this.taskPhase) {
                case READ_PREPARE_RESPONSE: {
                    if (canReadPrepareResponse(cumulateBuffer)) {
                        // possibly PreparedStatement bind occur error
                        taskEnd = readPrepareResponse(cumulateBuffer, serverStatusConsumer);
                    }
                    if (this.taskPhase == TaskPhase.SUSPEND) {
                        assert this.packetPublisher == null;
                        assert this.bindPhase == BindPhase.WAIT_BIND;
                        taskEnd = true;
                    }
                    continueDecode = false;
                }
                break;
                case READ_EXECUTE_RESPONSE: {
                    // maybe modify this.phase
                    taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
                    continueDecode = !taskEnd
                            && this.taskPhase == TaskPhase.READ_EXECUTE_RESPONSE
                            && Packets.hasOnePacket(cumulateBuffer);
                }
                break;
                case READ_RESULT_SET: {
                    taskEnd = readResultSet(cumulateBuffer, serverStatusConsumer);
                    continueDecode = !taskEnd
                            && this.taskPhase == TaskPhase.READ_EXECUTE_RESPONSE
                            && Packets.hasOnePacket(cumulateBuffer);
                }
                break;
                case READ_RESET_RESPONSE: {
                    if (readResetResponse(cumulateBuffer, serverStatusConsumer)) {
                        taskEnd = true;
                    } else {
                        this.taskPhase = TaskPhase.EXECUTE;
                        taskEnd = executeNextGroup(); // execute command
                    }
                    continueDecode = false;
                }
                break;
                case END: // resume for closing statement
                    taskEnd = true;
                    break;
                default:
                    throw MySQLExceptions.unexpectedEnum(this.taskPhase);
            }
        }

        if (taskEnd && this.taskPhase != TaskPhase.SUSPEND) {
            this.taskPhase = TaskPhase.END;
            cancelTimeoutTaskIfNeed();
            switch (this.bindPhase) {
                case ABANDON_BIND:
                case ERROR_ON_BIND:
                case WAIT_BIND:
                    // no-op, here no downstream
                    break;
                default: {
                    if (hasError()) {
                        publishError(this.sink::error);
                    } else {
                        this.sink.complete();
                    }
                }
            } // switch

            closeStatementIfNeed();
            deleteBigColumnFileIfNeed();
        }
        return taskEnd;
    }


    @Override
    protected Action onError(Throwable e) {
        final Action action;
        switch (this.taskPhase) {
            case PREPARED:
                action = Action.TASK_END;
                break;
            case END: {
                throw new IllegalStateException("CLOSE_STMT command send error.", e);
            }// TODO fix me
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
        if (this.taskPhase != TaskPhase.END) {
            this.taskPhase = TaskPhase.END;
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
        this.taskPhase = TaskPhase.READ_EXECUTE_RESPONSE;
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
                this.taskPhase = TaskPhase.EXECUTE;
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

        if (this.nextGroupNeedReset) {
            executeReset();
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
            this.taskPhase = TaskPhase.READ_EXECUTE_RESPONSE;
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
        assertPhase(TaskPhase.READ_EXECUTE_RESPONSE);

        final int fetchSize = this.stmt.getFetchSize();
        final boolean taskEnd;
        if (fetchSize > 0 && this.rowMeta.getColumnCount() > 0) {
            final ByteBuf packet = this.adjutant.allocator().buffer(13);

            Packets.writeInt3(packet, 9);
            packet.writeByte(0); // reset sequence_id

            packet.writeByte(Packets.COM_STMT_FETCH);
            Packets.writeInt4(packet, this.statementId);
            Packets.writeInt4(packet, fetchSize);

            this.packetPublisher = Mono.just(packet);
            this.taskPhase = TaskPhase.READ_RESULT_SET;
            taskEnd = false;
        } else {
            taskEnd = true;
            // here bug or MySQL server status error.
            addError(new IllegalStateException("no more fetch."));
        }
        return taskEnd;
    }

    @Override
    int getStmtTimeout() {
        final ParamSingleStmt stmt = this.stmt;
        final int timeoutMills;
        if (stmt instanceof MySQLPrepareStmt && ((MySQLPrepareStmt) stmt).stmt == null) {
            timeoutMills = 0;
        } else {
            timeoutMills = stmt.getTimeout();
        }
        return timeoutMills;
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

    /**
     * @see #executeNextGroup()
     * @see #handleExecuteMessageErrorInEventLoop(Throwable)
     */
    private void executeReset() {
        assert this.nextGroupNeedReset;
        this.packetPublisher = Mono.just(createResetPacket());
        this.taskPhase = TaskPhase.READ_RESET_RESPONSE;
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
     * @see #executeQuery(ParamStmt, Function, Consumer)
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
        switch (this.bindPhase) {
            case WAIT_BIND: {
                try {
                    this.bindPhase = BindPhase.BIND_COMPLETE;
                    ((MySQLPrepareStmt) this.stmt).setStmt(stmt);
                    ((PrepareSink) this.sink).setSink(sink);

                    final TaskPhase oldTaskPhase = this.taskPhase;

                    this.taskPhase = TaskPhase.EXECUTE;
                    if (hasError()) {
                        publishError(sink::error);
                    } else if (executeNextGroup()) {
                        this.taskPhase = TaskPhase.END;
                        publishError(sink::error);
                    } else if (oldTaskPhase == TaskPhase.SUSPEND) {
                        this.taskPhase = TaskPhase.RESUME;
                        this.resume(sink::error);
                        startTimeoutTaskIfNeed();
                    } else {
                        this.taskPhase = TaskPhase.READ_EXECUTE_RESPONSE;
                        startTimeoutTaskIfNeed();
                    }
                } catch (Throwable e) {
                    // here bug
                    this.bindPhase = BindPhase.ERROR_ON_BIND;
                    this.taskPhase = TaskPhase.END;
                    addError(e);
                    sink.error(e);
                }
            }
            break;
            case ABANDON_BIND:
            case ERROR_ON_BIND:
            case BIND_COMPLETE: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} have ended,but reuse {}", this, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
            break;
            default:
                // no bug,never here
                sink.error(MySQLExceptions.unexpectedEnum(this.bindPhase));


        }/// switch

    }

    /**
     * @see #abandonBind()
     */
    private void abandonBindInEventLoop() {
        // error can't be emitted to sink ,because not actual sink now.
        switch (this.bindPhase) {
            case WAIT_BIND: {
                this.bindPhase = BindPhase.ABANDON_BIND;
                if (this.taskPhase == TaskPhase.SUSPEND && isNeedCloseStatement()) {
                    this.taskPhase = TaskPhase.RESUME;
                    this.resume(this::addError); // here, no downstream

                } else {
                    this.taskPhase = TaskPhase.END;
                }
            }
            break;
            case ABANDON_BIND:
            case ERROR_ON_BIND:
            case BIND_COMPLETE: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} have ended,but reuse {}", this, PreparedStatement.class.getName());
                }
                addError(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
            break;
            default:
                // no bug,never here
                throw MySQLExceptions.unexpectedEnum(this.bindPhase);

        } // switch
    }

    /**
     * @see #closeOnBindError(Throwable)
     */
    private void closeOnBindErrorInEventLoop(final Throwable error) {
        // error can't be emitted to sink ,because not actual sink now.
        switch (this.bindPhase) {
            case WAIT_BIND: {
                this.bindPhase = BindPhase.ERROR_ON_BIND;
                LOG.debug("bind occur error ", error);
                if (this.taskPhase == TaskPhase.SUSPEND && this.isNeedCloseStatement()) {
                    this.taskPhase = TaskPhase.RESUME;
                    this.resume(this::addError); // here, no downstream
                } else {
                    this.taskPhase = TaskPhase.END;
                }
            }
            break;
            case ABANDON_BIND:
            case ERROR_ON_BIND:
            case BIND_COMPLETE: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} have ended,but reuse {}", this, PreparedStatement.class.getName());
                }
            }
            break;
            default:
                // no bug,never here
                throw MySQLExceptions.unexpectedEnum(this.bindPhase);
        } // switch

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
            case MySQLServerException.ERROR_HEADER:
                canRead = true;
                break;
            case 0: {
                cumulateBuffer.skipBytes(4); //statementId
                final int numColumns, numParams, capability;
                numColumns = Packets.readInt2AsInt(cumulateBuffer);
                numParams = Packets.readInt2AsInt(cumulateBuffer);
                capability = this.capability;

                int packetNumber = 0;
                if (payloadLength >= 12) {
                    cumulateBuffer.skipBytes(2); // skip warning_count
                    if ((capability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) == 0
                            || cumulateBuffer.readByte() == RESULTSET_METADATA_FULL) {
                        packetNumber = numColumns + numParams;
                    }
                } else {
                    packetNumber = numColumns + numParams;
                }

                if (packetNumber > 0 && (capability & Capabilities.CLIENT_DEPRECATE_EOF) == 0) {
                    if (numParams > 0) {
                        packetNumber++;
                    }
                    if (numColumns > 0) {
                        packetNumber++;
                    }
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

        final int headFlag = Packets.getHeaderFlag(cumulateBuffer);
        final boolean taskEnd;
        switch (headFlag) {
            case MySQLServerException.ERROR_HEADER: {
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
                if (payloadLength >= 12) {
                    final int warnings = Packets.readInt2AsInt(cumulateBuffer);//6. warning_count
                    if (warnings > 0) {
                        this.warning = JdbdWarning.create(String.format("produce %s warnings", warnings), Collections.emptyMap());
                    }

                    hasMeta = (capability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) == 0
                            || cumulateBuffer.readByte() == RESULTSET_METADATA_FULL;
                } else {
                    hasMeta = (numParams + numColumns) > 0;
                }
                cumulateBuffer.readerIndex(payloadIndex + payloadLength); //avoid tailor filler.

                // below read param and column meta data
                final boolean readEof = (capability & Capabilities.CLIENT_DEPRECATE_EOF) == 0;

                if (numParams > 0 && hasMeta) {
                    final MySQLColumnMeta[] paramMetas = new MySQLColumnMeta[numParams];

                    final Set<Integer> unknownCollationSet;
                    unknownCollationSet = MySQLColumnMeta.readMetas(cumulateBuffer, paramMetas, this);
                    if (unknownCollationSet.size() > 0 && !containsError(UnrecognizedCollationException.class)) {
                        addError(MySQLExceptions.unrecognizedCollationError(unknownCollationSet));
                    }
                    this.paramMetas = paramMetas;
                    if (readEof) {
                        readEofOfMeta(cumulateBuffer, serverStatusConsumer);
                    }
                } else {
                    this.paramMetas = MySQLColumnMeta.EMPTY;
                }

                if (numColumns > 0 && hasMeta) {
                    final MySQLRowMeta resultMeta;
                    this.rowMeta = resultMeta = MySQLRowMeta.readForPrepare(cumulateBuffer, numColumns, this);
                    if (resultMeta.unknownCollationSet.size() > 0 && !containsError(UnrecognizedCollationException.class)) {
                        addError(MySQLExceptions.unrecognizedCollationError(resultMeta.unknownCollationSet));
                    }
                    if (readEof) {
                        readEofOfMeta(cumulateBuffer, serverStatusConsumer);
                    }
                } else {
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
     * this method will update {@link #taskPhase}.
     * </p>
     *
     * @return true : task end.
     * @see #readPrepareResponse(ByteBuf, Consumer)
     */
    private boolean handleReadPrepareComplete() {
        if (this.paramMetas == null) {
            throw new IllegalStateException("this.paramMetas is null.");
        }
        final ParamSingleStmt stmt = this.stmt;
        final boolean taskEnd;
        if (stmt instanceof PrepareStmt) {
            this.bindPhase = BindPhase.WAIT_BIND; // first modify phase to WAIT_FOR_BINDING
            ((PrepareSink) this.sink).statementSink.success(this);

            switch (this.bindPhase) {
                case BIND_COMPLETE:
                    taskEnd = false;
                    break;
                case WAIT_BIND:
                    taskEnd = true;
                    this.taskPhase = TaskPhase.SUSPEND;
                    break;
                case ABANDON_BIND:
                case ERROR_ON_BIND:
                    taskEnd = true;
                    break;
                default:
                    // no bug,never here
                    throw MySQLExceptions.unexpectedEnum(this.bindPhase);
            }

        } else {
            this.taskPhase = TaskPhase.EXECUTE;
            taskEnd = executeNextGroup();
        }
        return taskEnd;
    }


    /**
     * @see #decode(ByteBuf, Consumer)
     */
    private void closeStatementIfNeed() {
        LOG.debug("close statement id {}", this.statementId);
        this.packetPublisher = Mono.just(createCloseStatementPacket());
    }

    private boolean isNeedCloseStatement() {
        return true;
    }

    /**
     * @see #handleExecuteMessageError(Throwable)
     */
    private void handleExecuteMessageErrorInEventLoop(final Throwable error) {
        addError(error);
        final boolean needReset = this.nextGroupNeedReset;
        if (needReset) {
            executeReset();
        }

        if (this.inDecodeMethod()) {
            return;
        }

        if (!needReset && isNeedCloseStatement()) {
            activelySendPacketsAndEndTask(Mono.just(createCloseStatementPacket()));
        }
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
     * modify {@link #taskPhase}
     * </p>
     *
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     * @see #executeNextGroup()
     */
    private boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(TaskPhase.READ_EXECUTE_RESPONSE);

        final boolean traceEnabled = LOG.isTraceEnabled();
        if (traceEnabled) {
            LOG.trace("{} read execute response", this);
        }
        final int header = Packets.getHeaderFlag(cumulateBuffer);
        final boolean taskEnd;
        switch (header) {
            case MySQLServerException.ERROR_HEADER: {
                readErrorPacket(cumulateBuffer);
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                taskEnd = readUpdateResult(cumulateBuffer, serverStatusConsumer);
            }
            break;
            default: {
                this.taskPhase = TaskPhase.READ_RESULT_SET;
                taskEnd = readResultSet(cumulateBuffer, serverStatusConsumer);
            }
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
            case MySQLServerException.ERROR_HEADER: {
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


    private void assertPhase(TaskPhase expectedPhase) {
        if (this.taskPhase != expectedPhase) {
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

        @Override
        public boolean isSessionCreated() {
            return this.getStmt().isSessionCreated();
        }

        @Override
        public DatabaseSession databaseSession() {
            return this.getStmt().databaseSession();
        }

    }// MySQLPrepareStmt

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


    enum TaskPhase {

        NONE,
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

        @Deprecated
        READ_FETCH_RESPONSE,

        ERROR_ON_BINDING,
        ABANDON_BINDING,

        SUSPEND,

        RESUME,

        END
    }

    private enum BindPhase {
        NONE,
        WAIT_BIND,
        ERROR_ON_BIND,
        ABANDON_BIND,
        BIND_COMPLETE

    }


}
