/*
 * Copyright 2023-2043 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.type.Blob;
import io.jdbd.type.BlobPath;
import io.jdbd.type.Clob;
import io.jdbd.type.TextPath;
import io.jdbd.util.JdbdUtils;
import io.jdbd.vendor.result.ColumnMeta;
import io.jdbd.vendor.stmt.ParamValue;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.BufferedReader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.IntSupplier;

/**
 * @see ExecuteCommandWriter
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
            // here must async
            this.writer.adjutant.execute(() -> sendPathParameterInEventLoop(batchIndex, paramValue, sink));

        });
    }



    /*################################## blow private method ##################################*/


    private void sendPathParameterInEventLoop(final int batchIndex, final ParamValue paramValue,
                                              final FluxSink<ByteBuf> sink) {

        try {
            final Object value = paramValue.getNonNull();
            sendParameterValue(batchIndex, paramValue, sink, value);
        } catch (Throwable e) {
            sink.error(e);
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
        } else if (source instanceof TextPath) {
            sendTextFile(batchIndex, paramValue, (TextPath) source, sink);
        } else {
            final ColumnMeta meta = this.columnMetas[paramValue.getIndex()];
            throw MySQLExceptions.dontSupportParam(meta, paramValue.getNonNull(), null);
        }

    }


    private void sendBinaryFie(final int batchIndex, final ParamValue paramValue, final BlobPath blobPath,
                               final FluxSink<ByteBuf> sink) {

        final int paramIndex = paramValue.getIndex();
        final Path path = blobPath.value();
        try (FileChannel channel = FileChannel.open(path, JdbdUtils.openOptionSet(blobPath))) {
            final long totalSize;
            totalSize = channel.size();

            final int chunkSize = this.writer.fixedEnv.blobSendChunkSize;
            final IntSupplier sequenceId = this.writer.sequenceId;

            long restBytes = totalSize;
            ByteBuf packet;
            for (int length; restBytes > 0; restBytes -= length) {

                length = (int) Math.min(chunkSize, restBytes);
                packet = createLongDataPacket(paramIndex, length);

                packet.writeBytes(channel, length);
                this.writer.stmtTask.resetSequenceId(); // reset before send
                Packets.sendPackets(packet, sequenceId, sink);

                LOG.debug("{} BlobPath send chunk complete {} bytes about {} MB", path, length, length >> 20);

            }

            LOG.debug("{} BlobPath send complete,size about {} MB", path, totalSize >> 20);
            sink.complete();
        } catch (Throwable e) {
            if (MySQLExceptions.isByteBufOutflow(e)) {
                sink.error(MySQLExceptions.netPacketTooLargeError(e));
            } else {
                sink.error(MySQLExceptions.readLocalFileError(batchIndex, this.columnMetas[paramIndex], blobPath, e));
            }

        }
    }


    private void sendTextFile(final int batchIndex, final ParamValue paramValue, final TextPath textPath,
                              final FluxSink<ByteBuf> sink) {

        final Path path = textPath.value();

        final long totalSize;

        try {
            totalSize = Files.size(path);
        } catch (Throwable e) {
            sink.error(e);
            return;
        }

        final int paramIndex = paramValue.getIndex();

        ByteBuf packet = null;

        try (BufferedReader reader = JdbdUtils.newBufferedReader(textPath, 8192)) {

            final Charset clientCharset = this.writer.clientCharset;

            final CharBuffer charBuffer = CharBuffer.allocate(2048);

            final int chunkSize = this.writer.fixedEnv.blobSendChunkSize;
            final IntSupplier sequenceId = this.writer.sequenceId;
            final CharsetEncoder encoder = clientCharset.newEncoder();

            long aboutRestBytes = totalSize; // about
            packet = createLongDataPacket(paramIndex, (int) Math.min(chunkSize, aboutRestBytes));

            ByteBuffer byteBuffer;
            for (int restChunkBytes = chunkSize, encodedBytes; reader.read(charBuffer) > 0; ) {

                charBuffer.flip();
                byteBuffer = encoder.encode(charBuffer);
                charBuffer.clear();

                encodedBytes = byteBuffer.remaining();

                if (encodedBytes < restChunkBytes) {
                    packet.writeBytes(byteBuffer);
                    restChunkBytes -= encodedBytes;
                    aboutRestBytes -= encodedBytes;
                    continue;
                }


                for (int limit; encodedBytes >= restChunkBytes; ) {

                    if (encodedBytes == restChunkBytes) {
                        packet.writeBytes(byteBuffer);
                    } else {
                        limit = byteBuffer.limit();
                        byteBuffer.limit(byteBuffer.position() + restChunkBytes);

                        packet.writeBytes(byteBuffer);

                        byteBuffer.limit(limit);
                    }
                    this.writer.stmtTask.resetSequenceId();// reset before send
                    Packets.sendPackets(packet, sequenceId, sink);
                    LOG.debug("{} TextPath send chunk complete {} bytes {} about MB", path, chunkSize, chunkSize >> 20);

                    aboutRestBytes -= restChunkBytes;

                    if (aboutRestBytes < 1024) {
                        packet = createLongDataPacket(paramIndex, 1024);
                    } else {
                        packet = createLongDataPacket(paramIndex, (int) Math.min(chunkSize, aboutRestBytes));
                    }
                    restChunkBytes = chunkSize;
                    encodedBytes = byteBuffer.remaining();
                }

                if (byteBuffer.hasRemaining()) {
                    packet.writeBytes(byteBuffer);
                }

            } // out loop

            if (packet.readableBytes() > Packets.HEADER_SIZE) {
                this.writer.stmtTask.resetSequenceId();// reset before send
                Packets.sendPackets(packet, sequenceId, sink);
                packet = null;
            }
            LOG.debug("{} TextPath send complete,size about {} MB", textPath.value(), totalSize >> 20);

            sink.complete();
        } catch (Throwable e) {
            if (packet != null && packet.refCnt() > 0) {
                packet.release();
            }
            if (MySQLExceptions.isByteBufOutflow(e)) {
                sink.error(MySQLExceptions.netPacketTooLargeError(e));
            } else {
                sink.error(MySQLExceptions.readLocalFileError(batchIndex, this.columnMetas[paramIndex], textPath, e));
            }
        }

    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html">Protocol::COM_STMT_SEND_LONG_DATA</a>
     */
    private ByteBuf createLongDataPacket(final int parameterIndex, final int capacity) {
        final ByteBuf packet = this.writer.adjutant.allocator()
                .buffer(Packets.HEADER_SIZE + capacity, Integer.MAX_VALUE - 128);
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
            if (this.terminate) {
                return;
            }
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
            if (this.terminate) {
                return;
            }
            if (this.adjutant.inEventLoop()) {
                onErrorInEventLoop(error);
            } else {
                this.adjutant.execute(() -> onErrorInEventLoop(error));
            }
        }

        @Override
        public final void onComplete() {
            if (this.terminate) {
                return;
            }
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



        abstract void onNextInEventLoop(final T item);


        private void onCompleteInEventLoop() {
            if (this.terminate) {
                return;
            }
            this.terminate = true;

            final ByteBuf packet = this.packet;
            boolean error = false;
            if (packet != null) {
                if (packet.readableBytes() > Packets.HEADER_SIZE) {
                    this.parameterWriter.writer.stmtTask.resetSequenceId();
                    Packets.sendPackets(packet, this.parameterWriter.writer.sequenceId, this.sink);
                } else if (packet.refCnt() > 0) {
                    packet.release();
                }
                this.packet = null;
            }

            this.sink.complete();
            LOG.debug("{} complete", getClass().getSimpleName());
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
            LOG.debug("{} error", getClass().getSimpleName(), e);

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
            LOG.debug("{} downdown cancel", getClass().getSimpleName());

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
                this.parameterWriter.writer.stmtTask.resetSequenceId();
                Packets.sendPackets(packet, this.parameterWriter.writer.sequenceId, this.sink);

                this.restPayloadBytes = 0;
                this.packet = null;
                this.packet = packet = createPacket();
                restPayloadLength = this.restPayloadBytes;

            }

            this.restPayloadBytes = restPayloadLength;
        }


    }//ByteArraySubscription


}
