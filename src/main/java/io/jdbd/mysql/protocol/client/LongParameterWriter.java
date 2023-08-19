package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.statement.InOutParameter;
import io.jdbd.type.*;
import io.jdbd.vendor.result.ColumnMeta;
import io.jdbd.vendor.stmt.ParamValue;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.function.IntSupplier;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html">Protocol::COM_STMT_SEND_LONG_DATA</a>
 */
final class LongParameterWriter {


    static LongParameterWriter create(ExecuteCommandWriter writer) {
        return new LongParameterWriter(writer);
    }

    private static final Logger LOG = LoggerFactory.getLogger(LongParameterWriter.class);

    private final ExecuteCommandWriter writer;

    private final int statementId;

    private final MySQLColumnMeta[] columnMetas;

    private LongParameterWriter(ExecuteCommandWriter writer) {
        this.writer = writer;
        this.statementId = writer.stmtTask.getStatementId();
        this.columnMetas = writer.stmtTask.getParameterMetas();

    }


    Flux<ByteBuf> write(final int batchIndex, final ParamValue paramValue) {
        return Flux.create(sink -> {
            if (this.writer.adjutant.inEventLoop()) {
                sendPathParameterInEventLoop(batchIndex, paramValue, sink);
            } else {
                this.writer.adjutant.execute(() -> sendPathParameterInEventLoop(batchIndex, paramValue, sink));
            }

        });
    }



    /*################################## blow private method ##################################*/


    private void sendPathParameterInEventLoop(final int batchIndex, final ParamValue paramValue,
                                              final FluxSink<ByteBuf> sink) {
        final Object value = paramValue.getNonNull();
        if (value instanceof InOutParameter) {
            sendParameterValue(batchIndex, paramValue, sink, Objects.requireNonNull(((InOutParameter) value).value()));
        } else {
            sendParameterValue(batchIndex, paramValue, sink, value);
        }
    }


    private void sendParameterValue(final int batchIndex, final ParamValue paramValue,
                                    final FluxSink<ByteBuf> sink, final Object source) {

        if (source instanceof Blob) {
            Flux.from(((Blob) source).value())
                    .subscribe(new ByteArraySubscription(this, batchIndex, paramValue, sink));
        } else if (source instanceof BlobPath) {
            sendBinaryFie(batchIndex, paramValue, (BlobPath) source, sink);
        } else if (source instanceof Clob) {
            Flux.from(((Clob) source).value())
                    .map(s -> s.toString().getBytes(this.writer.clientCharset))
                    .subscribe(new ByteArraySubscription(this, batchIndex, paramValue, sink));
        } else if (source instanceof Text) {
            final Text text = (Text) source;
            Flux.from(text.value())
                    .subscribe(new TextSubscription(this, batchIndex, paramValue, text.charset(), sink));
        } else if (source instanceof TextPath) {
            sendTextFile(batchIndex, paramValue, (TextPath) source, sink);
        } else {
            final ColumnMeta meta = this.columnMetas[paramValue.getIndex()];
            throw MySQLExceptions.dontSupportParam(meta, paramValue.getNonNull(), null);
        }

    }


    private void sendBinaryFie(final int batchIndex, final ParamValue paramValue, final BlobPath blobPath,
                               final FluxSink<ByteBuf> sink) {

        try (FileChannel channel = FileChannel.open(blobPath.value(), MySQLBinds.openOptionSet(blobPath))) {
            final long totalSize;
            totalSize = channel.size();

            final int chunkSize = this.writer.fixedEnv.blobSendChunkSize, paramIndex = paramValue.getIndex();
            final IntSupplier sequenceId = this.writer.stmtTask::nextSequenceId;


            long restBytes = totalSize;
            ByteBuf packet;
            for (int length; restBytes > 0; restBytes -= length) {

                length = (int) Math.min(chunkSize, restBytes);
                packet = createLongDataPacket(paramIndex, length);

                packet.writeBytes(channel, length);

                Packets.sendPackets(packet, sequenceId, sink);

            }


        } catch (Throwable e) {
            if (MySQLExceptions.isByteBufOutflow(e)) {
                sink.error(MySQLExceptions.netPacketTooLargeError(e));
            } else {
                final ColumnMeta meta = this.columnMetas[paramValue.getIndex()];
                sink.error(MySQLExceptions.readLocalFileError(batchIndex, meta, blobPath, e));
            }

        }
    }


    private void sendTextFile(final int batchIndex, final ParamValue paramValue, TextPath textPath,
                              final FluxSink<ByteBuf> sink) {


        try (FileChannel channel = FileChannel.open(textPath.value(), MySQLBinds.openOptionSet(textPath))) {
            final long totalSize;
            totalSize = channel.size();

            final int chunkSize = this.writer.fixedEnv.blobSendChunkSize, paramIndex = paramValue.getIndex();
            final IntSupplier sequenceId = this.writer.stmtTask::nextSequenceId;
            final Charset textCharset = textPath.charset(), clientCharset = this.writer.clientCharset;
            final boolean sameCharset = textCharset.equals(clientCharset);


            final ByteBuffer buffer;
            if (sameCharset) {
                buffer = null;
            } else {
                buffer = ByteBuffer.allocate(1024);
            }

            long restBytes = totalSize;
            ByteBuf packet;
            for (int length; restBytes > 0; restBytes -= length) {

                length = (int) Math.min(chunkSize, restBytes);
                packet = createLongDataPacket(paramIndex, length);

                if (sameCharset) {
                    packet.writeBytes(channel, length);
                } else {
                    MySQLBinds.readFileAndWrite(channel, buffer, packet, length, textCharset, clientCharset);
                }

                Packets.sendPackets(packet, sequenceId, sink);

            }


        } catch (Throwable e) {
            if (MySQLExceptions.isByteBufOutflow(e)) {
                sink.error(MySQLExceptions.netPacketTooLargeError(e));
            } else {
                final ColumnMeta meta = this.columnMetas[paramValue.getIndex()];
                sink.error(MySQLExceptions.readLocalFileError(batchIndex, meta, textPath, e));
            }

        }

    }


    private ByteBuf createLongDataPacket(final int parameterIndex, final int capacity) {
        final ByteBuf packet = this.writer.adjutant.allocator()
                .buffer(Packets.HEADER_SIZE + capacity, this.writer.fixedEnv.maxAllowedPacket);
        packet.writeZero(Packets.HEADER_SIZE); // placeholder of header

        packet.writeByte(Packets.COM_STMT_SEND_LONG_DATA); //status
        Packets.writeInt4(packet, this.statementId); //statement_id
        Packets.writeInt2(packet, parameterIndex);//param_id
        return packet;

    }



    /*################################## blow private instance inner class ##################################*/


    @SuppressWarnings("all")
    private static abstract class PacketSubscription<T> implements Subscription, Subscriber<T> {

        final LongParameterWriter parameterWriter;

        final TaskAdjutant adjutant;

        final int batchIndex;

        final ParamValue paramValue;

        final int paramIndex;

        final FluxSink<ByteBuf> sink;

        ByteBuf packet;

        int restPayloadBytes;

        int totalBytes = 0;

        private Subscription upstream;

        boolean terminate;

        private PacketSubscription(LongParameterWriter parameterWriter, int batchIndex,
                                   ParamValue paramValue, FluxSink<ByteBuf> sink) {
            this.parameterWriter = parameterWriter;
            this.adjutant = parameterWriter.writer.adjutant;
            this.batchIndex = batchIndex;
            this.paramValue = paramValue;

            this.paramIndex = paramValue.getIndex();
            this.sink = sink;

            this.restPayloadBytes = 0;
            this.packet = null;
            this.packet = createPacket();
        }

        @Override
        public final void request(long n) {
            final Subscription s = this.upstream;
            if (s != null) {
                s.request(n);
            }

        }

        @Override
        public final void cancel() {
            final Subscription s = this.upstream;
            if (s != null) {
                s.cancel();
            }
            if (this.adjutant.inEventLoop()) {
                downstreamOnCancel();
            } else {
                this.adjutant.execute(this::downstreamOnCancel);
            }

        }

        @Override
        public final void onSubscribe(Subscription s) {
            this.upstream = s;
            s.request(Long.MAX_VALUE);
        }

        @Override
        public final void onNext(final T item) {
            if (this.terminate) {
                return;
            }
            if (this.adjutant.inEventLoop()) {
                onNextInEventLoop(item);
            } else {
                this.adjutant.execute(() -> onNextInEventLoop(item));
            }
        }

        @Override
        public final void onError(final Throwable error) {
            if (this.adjutant.inEventLoop()) {
                onErrorInEventLoop(error);
            } else {
                this.adjutant.execute(() -> onErrorInEventLoop(error));
            }
        }

        @Override
        public final void onComplete() {
            if (this.adjutant.inEventLoop()) {
                onCompleteInEventLoop();
            } else {
                this.adjutant.execute(this::onCompleteInEventLoop);
            }
        }

        final ByteBuf createPacket() {
            assert this.restPayloadBytes == 0 && this.packet == null;

            final int chunkSize = this.parameterWriter.writer.fixedEnv.blobSendChunkSize;
            this.restPayloadBytes = chunkSize;
            final ByteBuf packet;
            packet = this.parameterWriter.createLongDataPacket(this.paramIndex, chunkSize);
            this.packet = packet;
            return packet;
        }

        final boolean sendPackets(final ByteBuf packet) {
            int totalBytes = this.totalBytes;
            totalBytes += (packet.readableBytes() - Packets.HEADER_SIZE);

            this.totalBytes = totalBytes;
            final boolean error = totalBytes > this.parameterWriter.writer.fixedEnv.maxAllowedPacket;

            if (error) {
                this.onErrorInEventLoop(MySQLExceptions.netPacketTooLargeError(null));
                if (packet.refCnt() > 0) {
                    packet.release();
                }
            } else {
                Packets.sendPackets(packet, this.parameterWriter.writer.sequenceId, this.sink);
            }
            return error;
        }


        abstract void onNextInEventLoop(final T item);


        private void onCompleteInEventLoop() {
            final ByteBuf packet = this.packet;
            boolean error = false;
            if (packet != null) {
                if (packet.readableBytes() > Packets.HEADER_SIZE) {
                    error = sendPackets(packet);
                } else {
                    packet.release();
                }
                this.packet = null;
            }
            if (!error) {
                this.sink.complete();
            }


        }


        private void onErrorInEventLoop(final Throwable e) {
            if (this.terminate) {
                return;
            }
            this.terminate = true;
            final ByteBuf packet = this.packet;
            if (packet != null && packet.refCnt() > 0) {
                packet.release();
                this.packet = null;
            }
            cancelSubscribeOnError();

            if (MySQLExceptions.isByteBufOutflow(e)) {
                this.sink.error(MySQLExceptions.netPacketTooLargeError(e));
            } else {
                this.sink.error(MySQLExceptions.longDataReadException(this.batchIndex, this.paramValue, e));
            }

        }

        private void cancelSubscribeOnError() {
            final Subscription s = this.upstream;
            if (s == null) {
                return;
            }
            try {
                s.cancel();
            } catch (Throwable e) {
                // Subscription.cannel() shouldn't throw error.
                LOG.debug("subscription cancel() throw error", e);
            }

        }

        private void downstreamOnCancel() {
            if (this.terminate) {
                return;
            }
            this.terminate = true;
            final ByteBuf packet = this.packet;
            if (packet != null && packet.refCnt() > 0) {
                packet.release();
                this.packet = null;
            }

        }

    }// PacketSubscription


    private static final class ByteArraySubscription extends PacketSubscription<byte[]> {


        private ByteArraySubscription(LongParameterWriter parameterWriter, int batchIndex, ParamValue paramValue,
                                      FluxSink<ByteBuf> sink) {
            super(parameterWriter, batchIndex, paramValue, sink);
        }


        @Override
        void onNextInEventLoop(final byte[] item) {
            if (this.terminate) {
                return;
            }
            ByteBuf packet = this.packet;

            int restPayloadLength = this.restPayloadBytes;
            for (int offset = 0, length, restLength = item.length; restLength > 0; restLength -= length) {
                length = Math.min(restPayloadLength, restLength);
                packet.writeBytes(item, offset, length);

                offset += length;
                restPayloadLength -= length;

                if (restPayloadLength > 0) {
                    continue;
                }
                if (sendPackets(packet)) {
                    break;
                }

                this.restPayloadBytes = 0;
                this.packet = null;
                packet = createPacket();
                restPayloadLength = this.restPayloadBytes;

            }

            this.restPayloadBytes = restPayloadLength;
        }


    }//ByteArraySubscription


    private static final class TextSubscription extends PacketSubscription<byte[]> {

        private final Charset textCharset;

        private TextSubscription(LongParameterWriter parameterWriter, int batchIndex, ParamValue paramValue,
                                 Charset textCharset, FluxSink<ByteBuf> sink) {
            super(parameterWriter, batchIndex, paramValue, sink);
            this.textCharset = textCharset;
        }

        @Override
        void onNextInEventLoop(final byte[] item) {
            if (this.terminate) {
                return;
            }
            final Charset textCharset = this.textCharset, clientCharset = this.parameterWriter.writer.clientCharset;

            final ByteBuffer buffer;
            buffer = clientCharset.encode(textCharset.decode(ByteBuffer.wrap(item)));

            ByteBuf packet = this.packet;

            int restPayloadLength = this.restPayloadBytes;
            for (int pos = buffer.position(), length, restLength = buffer.remaining(); restLength > 0; restLength -= length) {

                length = Math.min(restPayloadLength, restLength);

                buffer.position(pos);
                buffer.limit(pos + length);

                packet.writeBytes(buffer);

                pos += length;
                restPayloadLength -= length;

                if (restPayloadLength > 0) {
                    continue;
                }
                if (sendPackets(packet)) {
                    break;
                }

                this.restPayloadBytes = 0;
                this.packet = null;
                packet = createPacket();
                restPayloadLength = this.restPayloadBytes;

            }

            this.restPayloadBytes = restPayloadLength;
        }


    }//TextSubscription


}
