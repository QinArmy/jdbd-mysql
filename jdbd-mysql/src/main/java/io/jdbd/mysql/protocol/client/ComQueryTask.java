package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.stmt.BindBatchStmt;
import io.jdbd.mysql.stmt.BindMultiStmt;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.LocalFileException;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.result.FluxResultSink;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.ResultSetReader;
import io.jdbd.vendor.stmt.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * <p>
 * below is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
 */
final class ComQueryTask extends CommandTask {

    /*################################## blow StaticStatement underlying api method ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     *
     * @see #ComQueryTask(Stmt, FluxResultSink, TaskAdjutant)
     * @see ClientCommandProtocol#update(StaticStmt)
     */
    static Mono<ResultStates> update(final StaticStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
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
     *     <li>{@link StaticStatement#executeQuery(String)}</li>
     *     <li>{@link StaticStatement#executeQuery(String, Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see #ComQueryTask(Stmt, FluxResultSink, TaskAdjutant)
     * @see ClientCommandProtocol#query(StaticStmt)
     */
    static Flux<ResultRow> query(final StaticStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatch(List)} method.
     * </p>
     *
     * @see #ComQueryTask(Stmt, FluxResultSink, TaskAdjutant)
     * @see ClientCommandProtocol#batchUpdate(List)
     */
    static Flux<ResultStates> batchUpdate(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
        final Flux<ResultStates> flux;
        if (stmt.getSqlGroup().isEmpty()) {
            flux = Flux.error(MySQLExceptions.createEmptySqlException());
        } else {
            flux = MultiResults.batchUpdate(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
                }

            });
        }
        return flux;
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsMulti(List)} method.
     * </p>
     *
     * @see ClientCommandProtocol#executeAsMulti(List)
     * @see #ComQueryTask(Stmt, FluxResultSink, TaskAdjutant)
     */
    static MultiResult batchAsMulti(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
        final MultiResult result;
        if (stmt.getSqlGroup().isEmpty()) {
            result = MultiResults.error(MySQLExceptions.createEmptySqlException());
        } else {
            result = MultiResults.asMulti(adjutant, sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
                }
            });
        }
        return result;
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(List)} method.
     * </p>
     *
     * @see ClientCommandProtocol#executeAsFlux(List)
     * @see #ComQueryTask(Stmt, FluxResultSink, TaskAdjutant)
     */
    static OrderedFlux batchAsFlux(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
        final OrderedFlux flux;
        if (stmt.getSqlGroup().isEmpty()) {
            flux = MultiResults.orderedFluxError(MySQLExceptions.createEmptySqlException());
        } else {
            flux = MultiResults.asFlux(sink -> {
                try {
                    ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                    task.submit(sink::error);
                } catch (Throwable e) {
                    sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
                }
            });
        }
        return flux;
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(String)} method.
     * </p>
     */
    static OrderedFlux multiCommandAsFlux(StaticMultiStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    /*################################## blow BindableStatement underlying api method ##################################*/

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method.
     * </p>
     *
     * @see ComPreparedTask#update(ParamStmt, TaskAdjutant)
     * @see ClientCommandProtocol#bindableUpdate(BindStmt)
     */
    static Mono<ResultStates> bindableUpdate(final BindStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
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
     *     <li>{@link BindStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see ClientCommandProtocol#bindableQuery(BindStmt)
     */
    static Flux<ResultRow> bindableQuery(final BindStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatch()} method.
     * </p>
     *
     * @see ClientCommandProtocol#bindableBatch(BindBatchStmt)
     */
    static Flux<ResultStates> bindableBatch(final BindBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    /**
     * <p>
     * This method is one of underlying api of below methods {@link BindStatement#executeBatchAsMulti()}.
     * </p>
     */
    static MultiResult bindableAsMulti(final BindBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of below methods {@link BindStatement#executeBatchAsFlux()}.
     * </p>
     */
    static OrderedFlux bindableAsFlux(final BindBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /*################################## blow MultiStatement method ##################################*/


    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsMulti()} method.
     * </p>
     *
     * @see ClientCommandProtocol#multiStmtAsMulti(List)
     */
    static MultiResult multiStmtAsMulti(final BindMultiStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchAsFlux()} method.
     * </p>
     *
     * @see ClientCommandProtocol#multiStmtAsFlux(List)
     */
    static OrderedFlux multiStmtAsFlux(final BindMultiStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTask.class);

    private final Stmt stmt;

    private final FluxResultSink sink;

    private final ResultSetReader resultSetReader;

    private Phase phase;


    /**
     * <p>
     * This constructor create instance for {@link #update(StaticStmt, TaskAdjutant)}
     * </p>
     * <p>
     * The rule of {@link StaticStatement} underlying api constructor.
     *     <ul>
     *         <li>param 1 : stmt</li>
     *         <li>param 2 : sink</li>
     *         <li>param 3 : adjutant</li>
     *     </ul>
     * </p>
     *
     * @see #update(StaticStmt, TaskAdjutant)
     * @see #query(StaticStmt, TaskAdjutant)
     */
    private ComQueryTask(final Stmt stmt, FluxResultSink sink, TaskAdjutant adjutant) {
        super(adjutant, sink);
        if (!Capabilities.supportMultiStatement(adjutant.negotiatedCapability())) {
            throw new MySQLJdbdException("negotiatedCapability not support multi statement.");
        }
        this.stmt = stmt;
        this.sink = sink;
    }


    @Override
    public final String toString() {
        return this.getClass().getSimpleName() + "@" + this.hashCode();
    }


    /*################################## blow package template method ##################################*/

    @Nullable
    @Override
    protected final Publisher<ByteBuf> start() {
        Publisher<ByteBuf> publisher;
        final Stmt stmt = this.stmt;
        final Supplier<Integer> sequenceId = this::addAndGetSequenceId;
        try {
            if (stmt instanceof StaticStmt) {
                final String sql = ((StaticStmt) stmt).getSql();
                publisher = QueryCommandWriter.createStaticCommand(sql, sequenceId, this.adjutant);
            } else if (stmt instanceof StaticBatchStmt) {
                final StaticBatchStmt batchStmt = (StaticBatchStmt) stmt;
                publisher = QueryCommandWriter.createStaticBatchCommand(batchStmt, sequenceId, this.adjutant);
            } else if (stmt instanceof StaticMultiStmt) {
                final String sql = ((StaticMultiStmt) stmt).getMultiSql();
                publisher = QueryCommandWriter.createStaticCommand(sql, sequenceId, this.adjutant);
            } else if (stmt instanceof BindStmt) {
                publisher = QueryCommandWriter.createBindableCommand((BindStmt) stmt, sequenceId, this.adjutant);
            } else if (stmt instanceof BindBatchStmt) {
                final BindBatchStmt batchStmt = (BindBatchStmt) stmt;
                publisher = QueryCommandWriter.createBindableBatchCommand(batchStmt, sequenceId, this.adjutant);
            } else if (stmt instanceof BindMultiStmt) {
                final BindMultiStmt multiStmt = (BindMultiStmt) stmt;
                publisher = QueryCommandWriter.createBindableMultiCommand(multiStmt, sequenceId, this.adjutant);
            } else {
                throw new IllegalStateException(String.format("Unknown stmt[%s]", stmt.getClass().getName()));
            }
            this.phase = Phase.READ_EXECUTE_RESPONSE;
        } catch (Throwable e) {
            this.phase = Phase.START_ERROR;
            publisher = null;
            if (MySQLExceptions.isByteBufOutflow(e)) {
                addError(MySQLExceptions.tooLargeObject(e));
            } else {
                addError(e);
            }
        }
        return publisher;
    }


    @Override
    protected final boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        if (this.phase == Phase.START_ERROR) {
            publishError(this.sink::error);
            return true;
        }

        boolean taskEnd = false, continueRead = Packets.hasOnePacket(cumulateBuffer);
        while (continueRead) {
            switch (this.phase) {
                case READ_EXECUTE_RESPONSE: {
                    taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
                    continueRead = !taskEnd && Packets.hasOnePacket(cumulateBuffer);
                }
                break;
                case READ_TEXT_RESULT_SET: {
                    taskEnd = readTextResultSet(cumulateBuffer, serverStatusConsumer);
                    continueRead = !taskEnd && Packets.hasOnePacket(cumulateBuffer);
                }
                break;
                case READ_MULTI_STMT_ENABLE_RESULT: {
                    taskEnd = readEnableMultiStmtResponse(cumulateBuffer, serverStatusConsumer);
                    if (!taskEnd) {
                        this.phase = Phase.READ_EXECUTE_RESPONSE;
                    }
                    continueRead = false;
                }
                break;
                case READ_MULTI_STMT_DISABLE_RESULT: {
                    readDisableMultiStmtResponse(cumulateBuffer, serverStatusConsumer);
                    taskEnd = true;
                    continueRead = false;
                }
                break;
                case LOCAL_INFILE_REQUEST: {
                    throw new IllegalStateException(String.format("%s phase[%s] error.", this, this.phase));
                }
                default:
                    throw MySQLExceptions.createUnexpectedEnumException(this.phase);
            }
        }
        if (taskEnd) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("COM_QUERY instant[{}] task end.", this);
            }
            this.phase = Phase.TASK_END;
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
        if (this.phase == Phase.TASK_END) {
            LOG.error("Unknown error.", e);
        } else {
            this.phase = Phase.TASK_END;
            addError(MySQLExceptions.wrap(e));
            this.downstreamSink.error(createException());
        }
        return Action.TASK_END;
    }


    /*################################## blow private method ##################################*/

    /**
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean readEnableMultiStmtResponse(final ByteBuf cumulateBuffer
            , final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_MULTI_STMT_ENABLE_RESULT);

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        cumulateBuffer.skipBytes(1); // skip sequence id

        final int status = Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex());
        boolean taskEnd;
        switch (status) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} COM_SET_OPTION enable failure,{}", this, error);
                }
                // release ByteBuf
                Flux.from(Objects.requireNonNull(this.packetPublisher, "this.packetPublisher"))
                        .map(ByteBuf::release)
                        .subscribe();
                this.packetPublisher = null;
                this.tempMultiStmtStatus = TempMultiStmtStatus.ENABLE_FAILURE;
                addError(MySQLExceptions.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case EofPacket.EOF_HEADER:
            case OkPacket.OK_HEADER: {
                OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                this.tempMultiStmtStatus = TempMultiStmtStatus.ENABLE_SUCCESS;
                taskEnd = false;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} COM_SET_OPTION enable success.", this);
                }
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("COM_SET_OPTION response status[%s] error.", status);
        }
        return taskEnd;
    }


    /**
     * @see #decode(ByteBuf, Consumer)
     */
    private void readDisableMultiStmtResponse(final ByteBuf cumulateBuffer
            , final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_MULTI_STMT_DISABLE_RESULT);

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        cumulateBuffer.skipBytes(1); // skip sequence_id

        final int status = Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex());
        switch (status) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("COM_SET_OPTION disabled failure,{}", error);
                }
                this.tempMultiStmtStatus = TempMultiStmtStatus.DISABLE_FAILURE;
                addError(MySQLExceptions.createErrorPacketException(error));
            }
            break;
            case EofPacket.EOF_HEADER:
            case OkPacket.OK_HEADER: {
                OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("COM_SET_OPTION disabled success.");
                }
                this.tempMultiStmtStatus = TempMultiStmtStatus.DISABLE_SUCCESS;
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("COM_SET_OPTION response status[%s] error.", status);
        }
    }

    /**
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    private boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_EXECUTE_RESPONSE);

        final ComQueryResponse response = detectComQueryResponseType(cumulateBuffer, this.negotiatedCapability);
        boolean taskEnd = false;
        switch (response) {
            case ERROR: {
                readErrorPacket(cumulateBuffer);
                taskEnd = true;
            }
            break;
            case OK: {
                readUpdateResult(cumulateBuffer, serverStatusConsumer);
            }
            break;
            case LOCAL_INFILE_REQUEST: {
                sendLocalFile(cumulateBuffer);
                this.phase = Phase.READ_EXECUTE_RESPONSE;
            }
            break;
            case TEXT_RESULT: {
                this.phase = Phase.READ_TEXT_RESULT_SET;
                this.resultSetReader.read(cumulateBuffer, serverStatusConsumer);
                taskEnd = readTextResultSet(cumulateBuffer, serverStatusConsumer);

            }
            break;
            default:
                throw MySQLExceptions.createUnexpectedEnumException(response);
        }
        return taskEnd;
    }

    /**
     * <p>
     * when text result set end, update {@link #phase}.
     * </p>
     *
     * @return true: task end.
     */
    private boolean readTextResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_TEXT_RESULT_SET);
        final boolean taskEnd;
        if (this.downstreamSink.readResultSet(cumulateBuffer, serverStatusConsumer)) {
            if (this.downstreamSink.hasMoreResult()) {
                this.phase = Phase.READ_EXECUTE_RESPONSE;
                taskEnd = false;
            } else if (hasError()) {
                taskEnd = true;
            } else if (this.downstreamSink instanceof SingleModeBatchDownstreamSink) {
                final SingleModeBatchDownstreamSink sink = (SingleModeBatchDownstreamSink) this.downstreamSink;
                taskEnd = !sink.hasMoreGroup() || sink.sendCommand();
                if (!taskEnd) {
                    this.phase = Phase.READ_EXECUTE_RESPONSE;
                }
            } else {
                taskEnd = true;
            }
        } else {
            taskEnd = false;
        }
        return taskEnd;
    }


    /**
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html">Protocol::ERR_Packet</a>
     */
    private void sendLocalFile(final ByteBuf cumulateBuffer) {

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));

        if (Packets.readInt1AsInt(cumulateBuffer) != Packets.LOCAL_INFILE) {
            throw new IllegalStateException(String.format("%s invoke sendLocalFile method error.", this));
        }
        final String localFilePath;
        localFilePath = Packets.readStringFixed(cumulateBuffer, payloadLength, this.adjutant.charsetClient());

        final Path path = Paths.get(localFilePath);

        try {
            if (Files.notExists(path, LinkOption.NOFOLLOW_LINKS)) {
                String message = String.format("Local file[%s] not exits.", path);
                throw new LocalFileException(path, message);
            } else if (Files.isDirectory(path)) {
                String message = String.format("Local file[%s] isn directory.", path);
                throw new LocalFileException(path, message);
            } else if (!Files.isReadable(path)) {
                String message = String.format("Local file[%s] isn't readable.", path);
                throw new LocalFileException(path, message);
            } else {
                this.packetPublisher = Flux.create(sink -> {
                    if (this.adjutant.inEventLoop()) {
                        writeLocalFile(sink, path);
                    } else {
                        this.adjutant.execute(() -> writeLocalFile(sink, path));
                    }

                });
            }
        } catch (Throwable e) {
            if (e instanceof LocalFileException) {
                addError(e);
            } else {
                String message = String.format("Local file[%s] read occur error.", path);
                addError(new LocalFileException(path, message));
            }
            final ByteBuf packet = Packets.createEmptyPacket(this.adjutant.allocator(), addAndGetSequenceId());
            this.packetPublisher = Mono.just(packet);
        }


    }

    /**
     * @see #sendLocalFile(ByteBuf)
     */
    private void writeLocalFile(final FluxSink<ByteBuf> sink, final Path path) {

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {

            if (StandardCharsets.UTF_8.equals(this.adjutant.charsetClient())) {
                writeLocalFileBinary(path, channel, sink);
            } else {
                writeLocalFileText(path, channel, sink);
            }

        } catch (Throwable e) {
            if (e instanceof LocalFileException) {
                addError(e);
            } else {
                String m = String.format("Local file[%s] read occur error.", path);
                addError(new LocalFileException(path, 0L, m, e));
            }
            // send empty packet for end
            sink.next(Packets.createEmptyPacket(this.adjutant.allocator(), addAndGetSequenceId()));
        } finally {
            sink.complete();
        }


    }

    /**
     * @see #writeLocalFile(FluxSink, Path)
     */
    private void writeLocalFileBinary(final Path path, final FileChannel channel, final FluxSink<ByteBuf> sink)
            throws LocalFileException, IOException {

        final long fileSize = channel.size();
        long restFileBytes = fileSize;
        ByteBuf packet = null;
        try {
            final ByteBufAllocator allocator = this.adjutant.allocator();
            final byte[] bufferArray = new byte[2048];
            final ByteBuffer inputBuffer = ByteBuffer.wrap(bufferArray);
            int capacity;

            capacity = (int) Math.min(Packets.MAX_PACKET, Packets.HEADER_SIZE + restFileBytes);
            packet = allocator.buffer(capacity, Packets.MAX_PACKET);
            packet.writeZero(Packets.HEADER_SIZE);

            while (channel.read(inputBuffer) > 0) {
                inputBuffer.flip();
                final int readLength = inputBuffer.remaining();
                restFileBytes -= readLength;
                final int maxWritableBytes = packet.maxWritableBytes();
                if (readLength > maxWritableBytes) {
                    packet.writeBytes(bufferArray, 0, maxWritableBytes);
                    inputBuffer.position(maxWritableBytes); // modify position

                    Packets.writePacketHeader(packet, this.addAndGetSequenceId());
                    sink.next(packet);

                    capacity = (int) Math.min(Packets.MAX_PACKET, Packets.HEADER_SIZE + restFileBytes + inputBuffer.remaining());
                    packet = allocator.buffer(capacity, Packets.MAX_PACKET);
                    packet.writeZero(Packets.HEADER_SIZE);
                }
                packet.writeBytes(bufferArray, inputBuffer.position(), inputBuffer.remaining());
                inputBuffer.clear();
            }
            if (packet.readableBytes() > Packets.HEADER_SIZE) {
                Packets.writePacketHeader(packet, this.addAndGetSequenceId());
                sink.next(packet);
            } else {
                packet.release();
            }
            sink.next(Packets.createEmptyPacket(allocator, this.addAndGetSequenceId()));
        } catch (Throwable e) {
            if (packet != null) {
                packet.release();
            }
            final long sentBytes = fileSize - restFileBytes;
            final String message = String.format("Local file[%s] read error,have sent %s bytes.", path, sentBytes);
            throw new LocalFileException(path, sentBytes, message, e);
        }

    }


    /**
     * @see #writeLocalFile(FluxSink, Path)
     */
    private void writeLocalFileText(final Path path, final FileChannel channel, final FluxSink<ByteBuf> sink)
            throws LocalFileException, IOException {

        final long fileSize = channel.size();
        long restFileBytes = fileSize;
        ByteBuf packet = null;
        try {

            final ByteBufAllocator allocator = this.adjutant.allocator();
            final ByteBuffer inputBuffer = ByteBuffer.allocate(2048);
            int capacity, outLength, inLength, offset;

            capacity = (int) Math.min(Packets.MAX_PACKET, Packets.HEADER_SIZE + restFileBytes);
            packet = allocator.buffer(capacity, Packets.MAX_PACKET);
            packet.writeZero(Packets.HEADER_SIZE);
            ByteBuffer outBuffer;
            byte[] outArray;

            final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
            final CharsetEncoder encoder = this.adjutant.charsetClient().newEncoder();
            while (channel.read(inputBuffer) > 0) {
                inputBuffer.flip();
                inLength = inputBuffer.remaining();

                outBuffer = encoder.encode(decoder.decode(inputBuffer));
                outLength = outBuffer.remaining();
                if (outBuffer.hasArray()) {
                    outArray = outBuffer.array();
                } else {
                    outArray = new byte[outLength];
                    outBuffer.get(outArray);
                }
                offset = 0;
                final int maxWritableBytes = packet.maxWritableBytes();
                if (outLength > maxWritableBytes) {
                    packet.writeBytes(outArray, offset, maxWritableBytes);
                    offset += maxWritableBytes; // modify outLength
                    outLength -= maxWritableBytes;

                    Packets.writePacketHeader(packet, this.addAndGetSequenceId());
                    sink.next(packet);

                    capacity = (int) Math.min(Packets.MAX_PACKET, Packets.HEADER_SIZE + restFileBytes + outLength);
                    packet = allocator.buffer(capacity, Packets.MAX_PACKET);
                    packet.writeZero(Packets.HEADER_SIZE);
                }
                packet.writeBytes(outArray, offset, outLength);
                restFileBytes -= inLength;

                inputBuffer.clear();

            }
            if (packet.readableBytes() > Packets.HEADER_SIZE) {
                Packets.writePacketHeader(packet, this.addAndGetSequenceId());
                sink.next(packet);
            } else {
                packet.release();
            }
            sink.next(Packets.createEmptyPacket(allocator, this.addAndGetSequenceId()));
        } catch (Throwable e) {
            if (packet != null) {
                packet.release();
            }
            final long sentBytes = fileSize - restFileBytes;
            final String message = String.format("Local file[%s] read error,have sent %s bytes.", path, sentBytes);
            throw new LocalFileException(path, sentBytes, message, e);
        }
    }


    private void assertPhase(Phase expect) {
        if (this.phase != expect) {
            throw new IllegalStateException(String.format("%s current phase isn't %s .", this, expect));
        }
    }


    /**
     * invoke this method after invoke {@link Packets#hasOnePacket(ByteBuf)}.
     *
     * @see #decode(ByteBuf, Consumer)
     */
    static ComQueryResponse detectComQueryResponseType(final ByteBuf cumulateBuffer, final int negotiatedCapability) {
        int readerIndex = cumulateBuffer.readerIndex();
        final int payloadLength = Packets.getInt3(cumulateBuffer, readerIndex);
        // skip header
        readerIndex += Packets.HEADER_SIZE;
        ComQueryResponse responseType;
        final boolean metadata = (negotiatedCapability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;

        switch (Packets.getInt1AsInt(cumulateBuffer, readerIndex++)) {
            case 0: {
                if (metadata && Packets.obtainLenEncIntByteCount(cumulateBuffer, readerIndex) + 1 == payloadLength) {
                    responseType = ComQueryResponse.TEXT_RESULT;
                } else {
                    responseType = ComQueryResponse.OK;
                }
            }
            break;
            case ErrorPacket.ERROR_HEADER:
                responseType = ComQueryResponse.ERROR;
                break;
            case Packets.LOCAL_INFILE:
                responseType = ComQueryResponse.LOCAL_INFILE_REQUEST;
                break;
            default:
                responseType = ComQueryResponse.TEXT_RESULT;

        }
        return responseType;
    }

    /*################################## blow private static method ##################################*/


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    private enum ComQueryResponse {
        OK,
        ERROR,
        TEXT_RESULT,
        LOCAL_INFILE_REQUEST
    }


    private enum Phase {
        START_ERROR,
        READ_EXECUTE_RESPONSE,
        READ_TEXT_RESULT_SET,
        LOCAL_INFILE_REQUEST,
        READ_MULTI_STMT_ENABLE_RESULT,
        READ_MULTI_STMT_DISABLE_RESULT,
        TASK_END
    }


}
