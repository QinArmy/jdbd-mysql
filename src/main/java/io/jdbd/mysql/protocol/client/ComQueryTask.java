package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.*;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.MultiStatement;
import io.jdbd.statement.StaticStatement;
import io.jdbd.statement.StaticStatementSpec;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.stmt.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;

/**
 * <p>
 * This class is a implementation of {@link io.jdbd.vendor.task.CommunicationTask}.
 * This task is responsible for the communication about MySQL client COM_QUERY protocol .
 * </p>
 * <p>
 *     <ul>
 *         <li>{@link QueryCommandWriter} write  COM_QUERY packet</li>
 *         <li>{@link #readUpdateResult(ByteBuf, Consumer)} read result without return columns</li>
 *         <li>{@link #readResultSet(ByteBuf, Consumer)} read result with return column</li>
 *         <li>{@link #sendLocalFile(ByteBuf)} response LOCAL INFILE Data packet</li>
 *     </ul>
 * </p>
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
 */
final class ComQueryTask extends MySQLCommandTask {

    /*################################## blow StaticStatement spi underlying method ##################################*/

    /**
     * <p>
     * This method is underlying method of {@link StaticStatementSpec#executeUpdate(String)} spi method.
     * </p>
     *
     * @see #ComQueryTask(Stmt, ResultSink, TaskAdjutant)
     * @see ClientProtocol0#update(StaticStmt)
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
     * This method is underlying method of below spi methods:
     * <ul>
     *     <li>{@link StaticStatementSpec#executeQuery(String)}</li>
     *     <li>{@link StaticStatementSpec#executeQuery(String, Function)}</li>
     *     <li>{@link StaticStatementSpec#executeQuery(String, Function, Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see #ComQueryTask(Stmt, ResultSink, TaskAdjutant)
     * @see ClientProtocol0#query(StaticStmt, Function)
     */
    static <R> Flux<R> query(final StaticStmt stmt, final Function<CurrentRow, R> function,
                             final TaskAdjutant adjutant) {
        return MultiResults.query(function, stmt.getStatusConsumer(), sink -> {
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
     * This method is underlying api of {@link StaticStatement#executeBatchUpdate(List)} method.
     * </p>
     *
     * @see #ComQueryTask(Stmt, ResultSink, TaskAdjutant)
     * @see ClientProtocol0#batchUpdate(StaticBatchStmt)
     */
    static Flux<ResultStates> batchUpdate(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
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
     * This method is underlying api of {@link StaticStatement#executeBatchQuery(List)} method.
     * </p>
     *
     * @see #ComQueryTask(Stmt, ResultSink, TaskAdjutant)
     * @see MySQLProtocol#batchQuery(StaticBatchStmt)
     */
    static BatchQuery batchQuery(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.batchQuery(adjutant, sink -> {
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
     * This method is underlying api of {@link StaticStatement#executeBatchAsMulti(List)} method.
     * </p>
     *
     * @see ClientProtocol0#batchAsMulti(StaticBatchStmt)
     * @see #ComQueryTask(Stmt, ResultSink, TaskAdjutant)
     */
    static MultiResult batchAsMulti(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
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
     * This method is underlying api of {@link StaticStatement#executeBatchAsFlux(List)} method.
     * </p>
     *
     * @see ClientProtocol0#batchAsFlux(StaticBatchStmt)
     * @see #ComQueryTask(Stmt, ResultSink, TaskAdjutant)
     */
    static OrderedFlux batchAsFlux(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
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
     * This method is underlying api of {@link StaticStatement#executeAsFlux(String)} method.
     * </p>
     */
    static OrderedFlux executeAsFlux(StaticMultiStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    /*################################## blow BindStatement underlying api method ##################################*/

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method.
     * </p>
     *
     * @see ComPreparedTask#update(ParamStmt, TaskAdjutant)
     * @see ClientProtocol0#paramUpdate(ParamStmt, boolean)
     */
    static Mono<ResultStates> paramUpdate(final ParamStmt stmt, final TaskAdjutant adjutant) {
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
     *     <li>{@link BindStatement#executeQuery(Function)}</li>
     *     <li>{@link BindStatement#executeQuery(Function, Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see ClientProtocol0#paramQuery(ParamStmt, boolean, Function)
     */
    static <R> Flux<R> paramQuery(final ParamStmt stmt, final Function<CurrentRow, R> function,
                                  final TaskAdjutant adjutant) {
        return MultiResults.query(function, stmt.getStatusConsumer(), sink -> {
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
     * This method is one of underlying api of {@link BindStatement#executeBatchUpdate()} method.
     * </p>
     *
     * @see ClientProtocol0#paramBatchUpdate(ParamBatchStmt, boolean)
     */
    static Flux<ResultStates> paramBatchUpdate(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
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
     * This method is one of underlying api of following :
     * <ul>
     *     <li>{@link BindStatement#executeQuery()} </li>
     *     <li>{@link BindStatement#executeQuery(Function)}}</li>
     *     <li>{@link BindStatement#executeQuery(Function, Consumer)}</li>
     * </ul>
     * </p>
     */
    static BatchQuery paramBatchQuery(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.batchQuery(adjutant, sink -> {
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
    static MultiResult paramBatchAsMulti(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
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
    static OrderedFlux paramBatchAsFlux(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
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
     * This method is the underlying api of below methods {@link MultiStatement#executeBatchUpdate()}.
     * </p>
     */
    static Flux<ResultStates> multiStmtBatchUpdate(final ParamMultiStmt stmt, final TaskAdjutant adjutant) {
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
     * This method is the underlying api of below methods {@link MultiStatement#executeBatchQuery()}.
     * </p>
     */
    static BatchQuery multiStmtBatchQuery(final ParamMultiStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.batchQuery(adjutant, sink -> {
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
     * This method is underlying api of {@link MultiStatement#executeBatchAsMulti()} method.
     * </p>
     *
     * @see ClientProtocol0#multiStmtAsMulti(ParamMultiStmt)
     */
    static MultiResult multiStmtAsMulti(final ParamMultiStmt stmt, final TaskAdjutant adjutant) {
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
     */
    static OrderedFlux multiStmtAsFlux(final ParamMultiStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ComQueryTask task = new ComQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    private final Stmt stmt;

    private Phase phase;


    private ComQueryTask(final Stmt stmt, ResultSink sink, TaskAdjutant adjutant) {
        super(adjutant, sink);
        if (!Capabilities.supportMultiStatement(adjutant.capability())) {
            throw new JdbdException("negotiatedCapability not support multi statement.");
        }
        this.stmt = stmt;
    }





    /*################################## blow package template method ##################################*/

    @Nullable
    @Override
    protected Publisher<ByteBuf> start() {
        Publisher<ByteBuf> publisher;
        final Stmt stmt = this.stmt;

        try {
            final IntSupplier sequenceId = this::nextSequenceId;
            if (stmt instanceof StaticStmt || stmt instanceof StaticMultiStmt) {
                publisher = QueryCommandWriter.staticCommand(stmt, sequenceId, this.adjutant);
            } else if (stmt instanceof StaticBatchStmt) {
                final StaticBatchStmt batchStmt = (StaticBatchStmt) stmt;
                publisher = QueryCommandWriter.staticBatchCommand(batchStmt, sequenceId, this.adjutant);
            } else if (stmt instanceof ParamStmt) {
                publisher = QueryCommandWriter.bindableCommand((ParamStmt) stmt, sequenceId, this.adjutant);
            } else if (stmt instanceof ParamBatchStmt) {
                final ParamBatchStmt batchStmt = (ParamBatchStmt) stmt;
                publisher = QueryCommandWriter.bindableBatchCommand(batchStmt, sequenceId, this.adjutant);
            } else if (stmt instanceof ParamMultiStmt) {
                final ParamMultiStmt multiStmt = (ParamMultiStmt) stmt;
                publisher = QueryCommandWriter.bindableMultiCommand(multiStmt, sequenceId, this.adjutant);
            } else {
                throw new IllegalStateException(String.format("Unknown stmt[%s]", stmt.getClass().getName()));
            }
            this.phase = Phase.READ_EXECUTE_RESPONSE;
        } catch (Throwable e) {
            if (log.isDebugEnabled()) {
                log.debug("create COM_QUERY packet error for {}", stmt.getClass().getName(), e);
            }
            this.phase = Phase.ERROR_ON_START;
            publisher = null;
            if (MySQLExceptions.isByteBufOutflow(e)) {
                addError(MySQLExceptions.tooLargeObject(e));
            } else {
                addError(e);
            }
        }
        return publisher;
    }


    /**
     * @return true : task end
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">COM_QUERY Response</a>
     */
    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        if (this.phase == Phase.ERROR_ON_START) {
            publishError(this.sink::error);
            return true;
        }

        boolean taskEnd = false, continueRead = Packets.hasOnePacket(cumulateBuffer);
        while (continueRead) {
            switch (this.phase) {
                case READ_EXECUTE_RESPONSE:
                    taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
                    break;
                case READ_TEXT_RESULT_SET:
                    taskEnd = readResultSet(cumulateBuffer, serverStatusConsumer);
                    break;
                default:
                    throw MySQLExceptions.unexpectedEnum(this.phase);
            }// switch

            //TODO 根据下面这个思想优化 jdbd-postgre
            continueRead = !taskEnd
                    && this.phase != Phase.READ_TEXT_RESULT_SET
                    && Packets.hasOnePacket(cumulateBuffer);

        }

        if (taskEnd) {
            if (log.isTraceEnabled()) {
                log.trace("COM_QUERY instant[{}] task end.", this);
            }
            this.phase = Phase.TASK_END;
            if (hasError()) {
                publishError(this.sink::error);
            } else {
                this.sink.complete();
            }
            deleteBigColumnFileIfNeed();
        }
        return taskEnd;
    }

    @Override
    protected Action onError(Throwable e) {
        if (this.phase == Phase.TASK_END) {
            log.error("Unknown error.", e);
        } else {
            this.phase = Phase.TASK_END;
            addError(MySQLExceptions.wrapIfNonJvmFatal(e));
            log.error("occur error ", e);
            publishError(this.sink::error);
        }
        deleteBigColumnFileIfNeed();
        return Action.TASK_END;
    }


    @Override
    protected void onChannelClose() {
        super.onChannelClose();
        deleteBigColumnFileIfNeed();
    }

    @Override
    void handleReadResultSetEnd() {
        this.phase = Phase.READ_EXECUTE_RESPONSE;
    }

    @Override
    ResultSetReader createResultSetReader() {
        return TextResultSetReader.create(this);
    }

    @Override
    boolean hasMoreGroup() {
        // always false
        return false;
    }

    @Override
    boolean executeNextGroup() {
        // here bug.
        throw new IllegalStateException("No next group.");
    }

    @Override
    boolean executeNextFetch() {
        // here bug or MySQL server status error.
        throw new IllegalStateException("No next fetch.");
    }

    /*################################## blow private method ##################################*/


    /**
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    private boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assert this.phase == Phase.READ_EXECUTE_RESPONSE;

        final ComQueryResponse response;
        response = detectComQueryResponseType(cumulateBuffer, this.capability);
        boolean taskEnd = false;
        switch (response) {
            case ERROR: {
                readErrorPacket(cumulateBuffer);
                taskEnd = true;
            }
            break;
            case OK:
                taskEnd = readUpdateResult(cumulateBuffer, serverStatusConsumer);
                break;
            case LOCAL_INFILE_REQUEST: {
                sendLocalFile(cumulateBuffer);
                this.phase = Phase.READ_EXECUTE_RESPONSE;
            }
            break;
            case TEXT_RESULT: {
                this.phase = Phase.READ_TEXT_RESULT_SET;
                taskEnd = readResultSet(cumulateBuffer, serverStatusConsumer);
            }
            break;
            default:
                throw MySQLExceptions.unexpectedEnum(response);
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
                throw new JdbdException(message);
            } else if (Files.isDirectory(path)) {
                String message = String.format("Local file[%s] isn directory.", path);
                throw new JdbdException(message);
            } else if (!Files.isReadable(path)) {
                String message = String.format("Local file[%s] isn't readable.", path);
                throw new JdbdException(message);
            }
            this.packetPublisher = Flux.create(sink -> {
                if (this.adjutant.inEventLoop()) {
                    writeLocalFile(sink, path);
                } else {
                    this.adjutant.execute(() -> writeLocalFile(sink, path));
                }

            });
        } catch (Throwable e) {
            if (e instanceof JdbdException) {
                addError(e);
            } else {
                String message = String.format("Local file[%s] read occur error.", path);
                addError(new JdbdException(message));
            }
            final ByteBuf packet = Packets.createEmptyPacket(this.adjutant.allocator(), nextSequenceId());
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
            if (e instanceof JdbdException) {
                addError(e);
            } else {
                String m = String.format("Local file[%s] read occur error.", path);
                addError(new JdbdException(m));
            }
            // send empty packet for end
            sink.next(Packets.createEmptyPacket(this.adjutant.allocator(), nextSequenceId()));
        } finally {
            sink.complete();
        }


    }

    /**
     * @see #writeLocalFile(FluxSink, Path)
     */
    private void writeLocalFileBinary(final Path path, final FileChannel channel, final FluxSink<ByteBuf> sink)
            throws JdbdException, IOException {

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

                    Packets.writeHeader(packet, this.nextSequenceId());
                    sink.next(packet);

                    capacity = (int) Math.min(Packets.MAX_PACKET, Packets.HEADER_SIZE + restFileBytes + inputBuffer.remaining());
                    packet = allocator.buffer(capacity, Packets.MAX_PACKET);
                    packet.writeZero(Packets.HEADER_SIZE);
                }
                packet.writeBytes(bufferArray, inputBuffer.position(), inputBuffer.remaining());
                inputBuffer.clear();
            }
            if (packet.readableBytes() > Packets.HEADER_SIZE) {
                Packets.writeHeader(packet, this.nextSequenceId());
                sink.next(packet);
            } else {
                packet.release();
            }
            sink.next(Packets.createEmptyPacket(allocator, this.nextSequenceId()));
        } catch (Throwable e) {
            if (packet != null && packet.refCnt() > 0) {
                packet.release();
            }
            final long sentBytes = fileSize - restFileBytes;
            final String message = String.format("Local file[%s] read error,have sent %s bytes.", path, sentBytes);
            throw new JdbdException(message, e);
        }

    }


    /**
     * @see #writeLocalFile(FluxSink, Path)
     */
    private void writeLocalFileText(final Path path, final FileChannel channel, final FluxSink<ByteBuf> sink)
            throws JdbdException, IOException {

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

                    Packets.writeHeader(packet, this.nextSequenceId());
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
                Packets.writeHeader(packet, this.nextSequenceId());
                sink.next(packet);
            } else {
                packet.release();
            }
            sink.next(Packets.createEmptyPacket(allocator, this.nextSequenceId()));
        } catch (Throwable e) {
            if (packet != null) {
                packet.release();
            }
            final long sentBytes = fileSize - restFileBytes;
            final String message = String.format("Local file[%s] read error,have sent %s bytes.", path, sentBytes);
            throw new JdbdException(message, e);
        }
    }



    /*################################## blow private static method ##################################*/

    /**
     * invoke this method after invoke {@link Packets#hasOnePacket(ByteBuf)}.
     *
     * @see #decode(ByteBuf, Consumer)
     */
    private static ComQueryResponse detectComQueryResponseType(final ByteBuf cumulateBuffer, final int capability) {
        int readerIndex = cumulateBuffer.readerIndex();
        final int payloadLength;
        payloadLength = Packets.getInt3(cumulateBuffer, readerIndex);
        // skip header
        readerIndex += Packets.HEADER_SIZE;
        final ComQueryResponse responseType;
        final boolean metadata = (capability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;

        switch (Packets.getInt1AsInt(cumulateBuffer, readerIndex++)) {
            case 0: {
                if (metadata && Packets.getLenEnc(cumulateBuffer, readerIndex) + 2 == payloadLength) {
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

    /**
     * @see #detectComQueryResponseType(ByteBuf, int)
     */
    private static int obtainLenEncIntByteCount(ByteBuf byteBuf, final int index) {
        int byteCount;
        switch (Packets.getInt1AsInt(byteBuf, index)) {
            case Packets.ENC_0:
                throw MySQLExceptions.createFatalIoException("MyServer ComQuery response unknown packet", null);
            case Packets.ENC_3:
                byteCount = 3;
                break;
            case Packets.ENC_4:
                byteCount = 4;
                break;
            case Packets.ENC_9:
                byteCount = 9;
                break;
            default:
                // ENC_1
                byteCount = 1;
        }
        return byteCount;
    }


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
        ERROR_ON_START,
        READ_EXECUTE_RESPONSE,
        READ_TEXT_RESULT_SET,
        TASK_END
    }


}
