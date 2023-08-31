package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.statement.OutParameter;
import io.jdbd.statement.Parameter;
import io.jdbd.type.LongParameter;
import io.jdbd.vendor.stmt.*;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.function.IntSupplier;


/**
 * @see ComPreparedTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
 */
final class ExecuteCommandWriter extends BinaryWriter implements CommandWriter {


    static ExecuteCommandWriter create(final PrepareStmtTask stmtTask) {
        return new ExecuteCommandWriter(stmtTask);
    }


    // below enum_cursor_type @see https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a3e5e9e744ff6f7b989a604fd669977da
    private static final byte CURSOR_TYPE_READ_ONLY = 1;
    private static final byte PARAMETER_COUNT_AVAILABLE = 1 << 3;

    final PrepareStmtTask stmtTask;

    final IntSupplier sequenceId;

    final ParamSingleStmt stmt;

    final boolean supportQueryAttr;

    final boolean supportZoneOffset;

    final FixedEnv fixedEnv;

    final ZoneOffset serverZone;

    final Charset clientCharset;

    private LongParameterWriter longParamWriter;


    private ExecuteCommandWriter(final PrepareStmtTask stmtTask) {
        super(stmtTask.adjutant());
        this.stmtTask = stmtTask;
        this.sequenceId = stmtTask::nextSequenceId;
        this.stmt = stmtTask.getStmt();


        final TaskAdjutant adjutant = this.adjutant;
        this.supportQueryAttr = Capabilities.supportQueryAttr(adjutant.capability());
        this.supportZoneOffset = adjutant.handshake10().serverVersion.isSupportZoneOffset();
        this.fixedEnv = adjutant.getFactory();
        this.serverZone = adjutant.serverZone();
        this.clientCharset = adjutant.charsetClient();

    }


    @Override
    public Publisher<ByteBuf> writeCommand(final int batchIndex) throws JdbdException {
        final List<ParamValue> bindGroup;
        bindGroup = getBindGroup(batchIndex);

        final MySQLColumnMeta[] paramMetaArray = Objects.requireNonNull(this.stmtTask.getParameterMetas());
        MySQLBinds.assertParamCountMatch(batchIndex, paramMetaArray.length, bindGroup.size());

        List<ParamValue> longParamList = null;
        ParamValue paramValue;
        Object value;
        for (int i = 0; i < paramMetaArray.length; i++) {
            paramValue = bindGroup.get(i);
            if (paramValue.getIndex() != i) {
                // hear invoker has bug
                throw MySQLExceptions.bindValueParamIndexNotMatchError(batchIndex, paramValue, i);
            }
            value = paramValue.get();
            if (value instanceof LongParameter) {
                if (longParamList == null) {
                    longParamList = MySQLCollections.arrayList();
                }
                longParamList.add(paramValue);
            }
        }
        final Publisher<ByteBuf> publisher;
        if (paramMetaArray.length == 0 && (!this.supportQueryAttr || this.stmt.getStmtVarList().size() == 0)) {
            // this 'if' block handle no bind parameter.
            final ByteBuf packet;
            packet = createExecutePacket(10);
            Packets.writeIntLenEnc(packet, 0); // parameter_count
            this.stmtTask.resetSequenceId(); // reset sequenceId before write header
            publisher = Packets.createPacketPublisher(packet, this.sequenceId, this.adjutant);
        } else if (longParamList == null || longParamList.size() == 0) {
            // this 'if' block handle no long parameter.
            publisher = bindParameters(batchIndex, bindGroup);
        } else {
            LongParameterWriter longParamWriter = this.longParamWriter;
            if (longParamWriter == null) {
                this.longParamWriter = longParamWriter = LongParameterWriter.create(this);
            }
            this.stmtTask.nextGroupReset(); // next group need to reset
            final LongParameterWriter longWriter = longParamWriter;
            publisher = Flux.fromIterable(longParamList)
                    .flatMap(param -> longWriter.write(batchIndex, param))
                    .concatWith(defferBindParameters(batchIndex, bindGroup))
                    .onErrorResume(this::handleSendError);
        }
        return publisher;
    }

    /*################################## blow private method ##################################*/

    private <T> Publisher<T> handleSendError(final Throwable e) {
        final Mono<T> empty;
        if (this.adjutant.inEventLoop()) {
            this.stmtTask.addErrorToTask(MySQLExceptions.wrap(e));
            this.stmtTask.handleExecuteMessageError();
            empty = Mono.empty();
        } else {
            empty = Mono.create(sink -> this.adjutant.execute(() -> {
                this.stmtTask.addErrorToTask(MySQLExceptions.wrap(e));
                this.stmtTask.handleExecuteMessageError();
                sink.success();
            }));
        }
        return empty;
    }


    private List<ParamValue> getBindGroup(final int batchIndex) {
        ParamSingleStmt stmt = this.stmt;
        if (stmt instanceof PrepareStmt) {
            stmt = ((PrepareStmt) stmt).getStmt();
        }
        final List<ParamValue> bindGroup;
        if (stmt instanceof ParamStmt) {
            if (batchIndex > -1) {
                String m = String.format("batchIndex[%s] isn't negative for stmt[%s].", batchIndex, stmt);
                throw new IllegalArgumentException(m);
            }
            bindGroup = ((ParamStmt) stmt).getParamGroup();
        } else if (stmt instanceof ParamBatchStmt) {
            final ParamBatchStmt batchStmt = (ParamBatchStmt) stmt;
            final List<List<ParamValue>> groupList = batchStmt.getGroupList();
            if (batchIndex >= groupList.size()) {
                String m = String.format("batchIndex[%s] great or equal than group size[%s] for stmt[%s]."
                        , batchIndex, batchStmt.getGroupList().size(), stmt);
                throw new IllegalArgumentException(m);
            }
            bindGroup = groupList.get(batchIndex);
        } else {
            // here bug
            String m = String.format("Unknown stmt type %s", stmt);
            throw new IllegalStateException(m);
        }
        return bindGroup;
    }


    private Publisher<ByteBuf> defferBindParameters(final int batchIndex, final List<ParamValue> bindGroup) {
        return Flux.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                defferBIndParamInEventLoop(batchIndex, bindGroup, sink);
            } else {
                this.adjutant.execute(() -> defferBIndParamInEventLoop(batchIndex, bindGroup, sink));
            }
        });
    }

    private void defferBIndParamInEventLoop(final int batchIndex, final List<ParamValue> bindGroup,
                                            final FluxSink<ByteBuf> sink) {

        Publisher<ByteBuf> publisher;
        try {
            publisher = bindParameters(batchIndex, bindGroup);

        } catch (Throwable e) {
            publisher = null;
            sink.error(MySQLExceptions.wrap(e));
        }
        if (publisher != null) {
            Flux.from(publisher)
                    .subscribe(sink::next, sink::error, sink::complete);
        }

    }


    /**
     * @return {@link Flux} that is created by {@link Flux#fromIterable(Iterable)} method.
     * @see #writeCommand(int)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
     */
    private Publisher<ByteBuf> bindParameters(final int batchIndex, final List<ParamValue> paramGroup)
            throws JdbdException {

        final MySQLColumnMeta[] paramMetaArray = this.stmtTask.getParameterMetas();
        MySQLBinds.assertParamCountMatch(batchIndex, paramMetaArray.length, paramGroup.size());

        final ByteBuf packet;
        packet = createExecutePacket(1024);

        try {

            final List<NamedValue> queryAttrList = this.stmt.getStmtVarList();
            final int queryAttrSize = queryAttrList.size();
            final boolean supportQueryAttr = this.supportQueryAttr;

            final int paramCount;
            if (supportQueryAttr && queryAttrSize > 0) { //see createExecutePacket(), when only queryAttrSize > 0 PARAMETER_COUNT_AVAILABLE
                paramCount = paramMetaArray.length + queryAttrSize;
            } else {
                paramCount = paramMetaArray.length;
            }

            if (supportQueryAttr) {
                Packets.writeIntLenEnc(packet, paramCount); // parameter_count
            }

            final byte[] nullBitsMap = new byte[(paramCount + 7) >> 3];
            final int nullBitsMapIndex = packet.writerIndex();
            packet.writeZero(nullBitsMap.length); // placeholder for fill null_bitmap
            packet.writeByte(1); //fill new_params_bind_flag


            MySQLType type;
            ParamValue paramValue;
            Object value;
            //1. make nullBitsMap and fill  parameter_types
            final int anonymousParamCount = paramMetaArray.length;
            for (int i = 0; i < anonymousParamCount; i++) {
                paramValue = paramGroup.get(i);
                value = paramValue.get();
                if (value == null || value instanceof OutParameter) {
                    nullBitsMap[i >> 3] |= (1 << (i & 7));
                }
                type = decideActualType(paramValue);
                Packets.writeInt2(packet, type.parameterType);
                if (supportQueryAttr) {
                    packet.writeByte(0); //write empty, anonymous parameter, not query attribute parameter. string<lenenc>
                }
            }


            if (supportQueryAttr && queryAttrSize > 0) {
                writeQueryAttrType(packet, queryAttrList, nullBitsMap);
            }

            // write nullBitsMap
            Packets.writeBytesAtIndex(packet, nullBitsMap, nullBitsMapIndex);


            // write parameter value
            if (anonymousParamCount > 0) {
                for (int i = 0; i < anonymousParamCount; i++) {
                    paramValue = paramGroup.get(i);
                    value = paramValue.get();
                    if (value == null || value instanceof Parameter) {
                        continue;
                    }
                    writeBinary(packet, batchIndex, paramValue, paramMetaArray[i].getScale());
                }
            }

            //  write query attribute
            if (supportQueryAttr && queryAttrSize > 0) {
                NamedValue namedValue;
                for (int i = 0; i < queryAttrSize; i++) {
                    namedValue = queryAttrList.get(i);
                    if (namedValue.get() == null) {
                        continue;
                    }
                    writeBinary(packet, batchIndex, namedValue, paramMetaArray[i].getScale());
                }
            }

            this.stmtTask.resetSequenceId(); // reset sequenceId before write header
            return Packets.createPacketPublisher(packet, this.sequenceId, this.adjutant);

        } catch (Throwable e) {
            if (packet.refCnt() > 0) {
                packet.release();
            }
            throw MySQLExceptions.wrap(e);

        }
    }


    /**
     * @see #bindParameters(int, List)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
     */
    private ByteBuf createExecutePacket(final int capacity) {

        final ByteBuf packet;
        packet = this.adjutant.allocator().buffer(Packets.HEADER_SIZE + capacity, Integer.MAX_VALUE);
        packet.writeZero(Packets.HEADER_SIZE); // placeholder of header

        packet.writeByte(Packets.COM_STMT_EXECUTE); // 1.status
        Packets.writeInt4(packet, this.stmtTask.getStatementId());// 2. statement_id
        //3.cursor Flags, reactive api not support cursor
        int flags = 0;
        if (this.stmtTask.isSupportFetch()) {
            flags |= CURSOR_TYPE_READ_ONLY;
        }
        if (this.supportQueryAttr) {
            flags |= PARAMETER_COUNT_AVAILABLE;
        }
        packet.writeByte(flags); // flags
        Packets.writeInt4(packet, 1);//4. iteration_count,Number of times to execute the statement. Currently, always 1.

        return packet;
    }


}
