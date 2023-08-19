package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.type.BlobPath;
import io.jdbd.type.TextPath;
import io.jdbd.vendor.env.Environment;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.function.Consumer;

/**
 * <p>
 * This class is base class of following :
 *     <ul>
 *         <li>{@link TextResultSetReader}</li>
 *         <li>{@link BinaryResultSetReader}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
abstract class MySQLResultSetReader implements ResultSetReader {

    private static final Path TEMP_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"), "jdbd/mysql/big_row")
            .toAbsolutePath();


    static final Object MORE_CUMULATE_OBJECT = States.MORE_CUMULATE;

    final TaskAdjutant adjutant;

    final StmtTask task;

    final int capability;

    final FixedEnv fixedEnv;

    final Environment env;

    private Throwable error;

    private ByteBuf bigPayload;

    private MySQLCurrentRow currentRow;


    MySQLResultSetReader(StmtTask task) {
        this.task = task;
        this.adjutant = task.adjutant();
        this.capability = this.adjutant.capability();
        this.fixedEnv = this.adjutant.getFactory();
        this.env = this.fixedEnv.env;
    }

    @Override
    public final States read(ByteBuf cumulateBuffer, Consumer<Object> serverStatesConsumer)
            throws JdbdException {
        MySQLCurrentRow currentRow = this.currentRow;

        if (currentRow == null && (currentRow = readRowMeta(cumulateBuffer, serverStatesConsumer)) != null) {
            this.currentRow = currentRow;
            this.task.next(currentRow.rowMeta); // emit ResultRowMeta as the header of query result.
        }
        final States states;
        if (currentRow == null) {
            states = States.MORE_CUMULATE;
        } else {
            states = readRowSet(cumulateBuffer, serverStatesConsumer);
        }
        return states;
    }


    /*################################## blow packet template method ##################################*/


    @Nullable
    abstract MySQLCurrentRow readRowMeta(ByteBuf cumulateBuffer, Consumer<Object> serverStatesConsumer);


    abstract boolean readOneRow(ByteBuf cumulateBuffer, final boolean bigPayload, MySQLCurrentRow currentRow);


    abstract Logger getLogger();


    /*################################## blow final packet method ##################################*/


    /**
     * @return <ul>
     * <li>{@link #MORE_CUMULATE_OBJECT} : more cumulate</li>
     * <li>column value</li>
     * </ul>
     */
    final Object readLongText(final ByteBuf payload, final MySQLColumnMeta meta,
                              final MySQLCurrentRow currentRow) {
        BigColumn bigColumn = currentRow.bigColumn;
        final Object value;
        final int readableBytes;
        readableBytes = payload.readableBytes();

        final long lenEnc;
        final byte[] bytes;
        if (bigColumn != null) {
            if (readBigColumn(payload, bigColumn)) {
                value = TextPath.from(true, this.adjutant.columnCharset(meta.columnCharset), bigColumn.path);
            } else {
                value = MORE_CUMULATE_OBJECT;
            }
        } else if (readableBytes == 0) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((lenEnc = Packets.readLenEnc(payload)) <= readableBytes) {
            bytes = new byte[(int) lenEnc];
            payload.readBytes(bytes);
            value = new String(bytes, this.adjutant.columnCharset(meta.columnCharset));
        } else if (lenEnc <= this.fixedEnv.bigColumnBoundaryBytes) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((bigColumn = createBigColumnFile(meta.columnIndex, lenEnc)) == null) {
            payload.skipBytes(payload.readableBytes());
            value = MORE_CUMULATE_OBJECT;
        } else {
            currentRow.setBigColumn(bigColumn);
            readBigColumn(payload, bigColumn);
            value = MORE_CUMULATE_OBJECT;
        }
        return value;
    }

    /**
     * @return <ul>
     * <li>{@link #MORE_CUMULATE_OBJECT} : more cumulate</li>
     * <li>column value</li>
     * </ul>
     */
    final Object readLongBlob(final ByteBuf payload, final MySQLColumnMeta meta, final MySQLCurrentRow currentRow) {
        BigColumn bigColumn = currentRow.bigColumn;
        final Object value;
        final int readableBytes;
        readableBytes = payload.readableBytes();

        final long lenEnc;
        final byte[] bytes;
        if (bigColumn != null) {
            if (readBigColumn(payload, bigColumn)) {
                value = BlobPath.from(true, bigColumn.path);
            } else {
                value = MORE_CUMULATE_OBJECT;
            }
        } else if (readableBytes == 0) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((lenEnc = Packets.readLenEnc(payload)) <= readableBytes) {
            bytes = new byte[(int) lenEnc];
            payload.readBytes(bytes);
            value = bytes;
        } else if (lenEnc < this.fixedEnv.bigColumnBoundaryBytes) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((bigColumn = createBigColumnFile(meta.columnIndex, lenEnc)) == null) {
            payload.skipBytes(payload.readableBytes());
            value = MORE_CUMULATE_OBJECT;
        } else {
            currentRow.setBigColumn(bigColumn);
            readBigColumn(payload, bigColumn);
            value = MORE_CUMULATE_OBJECT;
        }
        return value;
    }


    /**
     * @return <ul>
     * <li>{@link #MORE_CUMULATE_OBJECT} : more cumulate</li>
     * <li>column value</li>
     * </ul>
     */
    final Object readGeometry(final ByteBuf payload, final MySQLColumnMeta meta, final MySQLCurrentRow currentRow) {
        BigColumn bigColumn = currentRow.bigColumn;
        final Object value;
        final int readableBytes;
        readableBytes = payload.readableBytes();

        final long lenEnc;
        final byte[] bytes;
        if (bigColumn != null) {
            if (readBigColumn(payload, bigColumn)) {
                value = BlobPath.from(true, bigColumn.path);
            } else {
                value = MORE_CUMULATE_OBJECT;
            }
        } else if (readableBytes < 5) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((lenEnc = Packets.readLenEnc(payload)) <= readableBytes) {
            payload.skipBytes(4);// skip geometry prefix
            bytes = new byte[((int) lenEnc) - 4];
            payload.readBytes(bytes);
            value = bytes;
        } else if (lenEnc < this.fixedEnv.bigColumnBoundaryBytes) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((bigColumn = createBigColumnFile(meta.columnIndex, lenEnc - 4)) == null) {
            payload.skipBytes(payload.readableBytes());
            value = MORE_CUMULATE_OBJECT;
        } else {
            currentRow.setBigColumn(bigColumn);
            payload.skipBytes(4);// skip geometry prefix
            readBigColumn(payload, bigColumn);
            value = MORE_CUMULATE_OBJECT;
        }
        return value;
    }


    final Object readBitType(final ByteBuf cumulateBuffer, final int readableBytes) {
        final int lenEnc;
        if (readableBytes == 0 || (lenEnc = Packets.readLenEncAsInt(cumulateBuffer)) > readableBytes) {
            return MORE_CUMULATE_OBJECT;
        }
        final byte[] bytes;
        bytes = new byte[lenEnc];
        cumulateBuffer.readBytes(bytes);
        return parseBitAsLong(bytes);
    }

    final long parseBitAsLong(final byte[] bytes) {
        long v = 0L;
        for (int i = 0, bits = (bytes.length - 1) << 3; i < bytes.length; i++, bits -= 8) {
            v |= ((bytes[i] & 0xFFL) << bits);
        }
        return v;
    }


    @Nullable
    final LocalDate handleZeroDateBehavior(String type) {
        final Enums.ZeroDatetimeBehavior behavior;
        behavior = this.env.getOrDefault(MySQLKey.ZERO_DATE_TIME_BEHAVIOR);
        final LocalDate date;
        switch (behavior) {
            case EXCEPTION: {
                String message = String.format("%s type can't is 0,@see jdbc url property[%s].",
                        type, MySQLKey.ZERO_DATE_TIME_BEHAVIOR);
                final Throwable error;
                error = MySQLExceptions.createTruncatedWrongValue(message, null);
                this.error = error;
                this.task.addErrorToTask(error);
                date = null;
            }
            break;
            case ROUND:
                date = LocalDate.of(1, 1, 1);
                break;
            case CONVERT_TO_NULL:
                date = null;
                break;
            default:
                throw MySQLExceptions.unexpectedEnum(behavior);
        }
        return date;
    }

    @Nullable
    final BigColumn createBigColumnFile(final int columnIndex, final long totalBytes) {
        Path path;
        try {
            path = Files.createTempFile(TEMP_DIRECTORY, "big_column", ".jdbd");
        } catch (Throwable e) {
            path = null;
            this.error = e;
            this.task.addErrorToTask(e);
        }
        if (path == null) {
            return null;
        }
        return new BigColumn(columnIndex, path, totalBytes);
    }


    /**
     * @return true : big column end
     */
    final boolean readBigColumn(final ByteBuf payload, final BigColumn bigColumn) {
        final long restBytes;
        restBytes = bigColumn.restBytes();

        if (restBytes == 0) {
            // last packet is max payload
            return true;
        }
        final int readableBytes, readLength;
        readableBytes = payload.readableBytes();
        if (restBytes < readableBytes) {
            readLength = (int) restBytes;
        } else {
            readLength = readableBytes;
        }

        if (this.task.isCancelled()) {
            payload.skipBytes(readLength);
            return bigColumn.writeBytes(readLength);
        }

        final int startIndex;
        startIndex = payload.readerIndex();
        Throwable error = null;
        try (FileChannel channel = FileChannel.open(bigColumn.path, StandardOpenOption.APPEND)) {

            payload.readBytes(channel, channel.position(), readLength);

        } catch (Throwable e) {
            this.error = error = e;
            payload.readerIndex(startIndex + readLength);
            this.task.addErrorToTask(e);
        }

        if (error != null) {
            try {
                Files.deleteIfExists(bigColumn.path);
            } catch (Throwable e) {
                this.task.addErrorToTask(e);
            }
        }
        return bigColumn.writeBytes(readLength);
    }




    /*################################## blow private method ##################################*/


    /**
     * @see #read(ByteBuf, Consumer)
     */
    private States readRowSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer) {
        final MySQLCurrentRow currentRow = this.currentRow;
        assert currentRow != null;
        final StmtTask task = this.task;

        States states = States.MORE_CUMULATE;
        ByteBuf payload;
        int sequenceId = -1;

        ByteBuf bigPayload = this.bigPayload;
        boolean resultSetEnd = false, oneRowEnd, cancelled;
        cancelled = task.isCancelled();

        outerLoop:
        for (int payloadLength, packetIndex, writerIndex = 0, limitIndex; Packets.hasOnePacket(cumulateBuffer); ) {
            packetIndex = cumulateBuffer.readerIndex();
            payloadLength = Packets.readInt3(cumulateBuffer);

            limitIndex = packetIndex + Packets.HEADER_SIZE + payloadLength;

            switch (Packets.getInt1AsInt(cumulateBuffer, packetIndex + Packets.HEADER_SIZE)) {
                case ErrorPacket.ERROR_HEADER: {
                    sequenceId = Packets.readInt1AsInt(cumulateBuffer);
                    final ErrorPacket packet;
                    packet = ErrorPacket.readCumulate(cumulateBuffer, payloadLength, this.capability,
                            this.adjutant.errorCharset());
                    this.task.addErrorToTask(MySQLExceptions.createErrorPacketException(packet));
                    states = States.END_ON_ERROR;
                    resultSetEnd = true;
                }
                break outerLoop;
                case EofPacket.EOF_HEADER: {
                    sequenceId = Packets.readInt1AsInt(cumulateBuffer);

                    final Terminator terminator;
                    terminator = Terminator.fromCumulate(cumulateBuffer, payloadLength, this.capability);
                    this.currentRow = null;
                    resultSetEnd = true;
                    serverStatesConsumer.accept(terminator);

                    if (!cancelled) {
                        task.next(MySQLResultStates.fromQuery(currentRow.getResultNo(), terminator, currentRow.rowCount));
                    }
                    if (terminator.hasMoreFetch()) {
                        states = States.MORE_FETCH;
                    } else if (terminator.hasMoreResult()) {
                        states = States.MORE_RESULT;
                    } else {
                        states = States.NO_MORE_RESULT;
                    }
                }
                break outerLoop;
                default: //no-op

            }

            if (cancelled) {
                sequenceId = Packets.readInt1AsInt(cumulateBuffer);
                cumulateBuffer.skipBytes(payloadLength);
                continue;
            }

            if (bigPayload == null && payloadLength < Packets.MAX_PAYLOAD) {
                sequenceId = Packets.readInt1AsInt(cumulateBuffer);
                writerIndex = cumulateBuffer.writerIndex();
                if (limitIndex != writerIndex) {
                    cumulateBuffer.writerIndex(limitIndex);
                }
                payload = cumulateBuffer;
            } else {
                if (bigPayload == null) {
                    this.bigPayload = bigPayload = this.adjutant.allocator()
                            .buffer(Packets.payloadBytesOfBigPacket(cumulateBuffer), Integer.MAX_VALUE);
                }
                payload = bigPayload;
                sequenceId = Packets.readBigPayload(cumulateBuffer, payload);
            }

            oneRowEnd = readOneRow(payload, payload != cumulateBuffer, currentRow);  // read one row

            if (payload == cumulateBuffer) {
                assert oneRowEnd; // fail ,driver bug or server bug
                if (limitIndex != writerIndex) {
                    assert writerIndex > limitIndex; // fail , driver bug.
                    cumulateBuffer.writerIndex(writerIndex);
                }
                cumulateBuffer.readerIndex(limitIndex); //avoid tailor filler
            } else if (oneRowEnd) {
                payload.release();
                this.bigPayload = bigPayload = null;
            } else if (payload.readableBytes() == 0) {
                payload.readerIndex(0);
                payload.writerIndex(0);
            } else if (payload.readerIndex() > 0) {
                payload.discardReadBytes();
            }

            if (!oneRowEnd) {
                // MORE_CUMULATE
                break;
            }

            if (!(cancelled = this.error != null)) {
                currentRow.rowCount++;
                task.next(currentRow);
                currentRow.reset();
            }

        } // outer loop

        if (sequenceId > -1) {
            this.task.updateSequenceId(sequenceId);
        }

        if (resultSetEnd) {
            // reset this instance
            bigPayload = this.bigPayload;
            if (bigPayload != null && bigPayload.refCnt() > 0) {
                bigPayload.release();
            }
            this.bigPayload = null;
            this.currentRow = null;
            this.error = null;
        }
        return states;
    }


    static abstract class MySQLCurrentRow extends MySQLRow.JdbdCurrentRow {

        private BigColumn bigColumn;

        private long rowCount = 0L;
        private boolean bigRow;

        MySQLCurrentRow(MySQLRowMeta rowMeta) {
            super(rowMeta);
        }

        @Override
        public final boolean isBigRow() {
            return this.bigRow;
        }

        @Override
        public final long rowNumber() {
            return this.rowCount;
        }

        final boolean isBigColumnNull() {
            return this.bigColumn == null;
        }

        final void setBigColumn(BigColumn bigColumn) {
            if (this.bigColumn != null) {
                throw new IllegalStateException();
            }
            this.bigColumn = bigColumn;
            this.bigRow = true;
        }

        final void reset() {
            bigRow = false;
            this.doRest();
        }

        abstract void doRest();


    }//MySQLCurrentRow

    static final class BigColumn {

        final int columnIndex;

        final Path path;

        private final long totalBytes;

        private long wroteBytes = 0L;

        private BigColumn(int columnIndex, Path path, long totalBytes) {
            this.columnIndex = columnIndex;
            this.path = path;
            this.totalBytes = totalBytes;
        }


        private long restBytes() {
            final long rest;
            rest = this.totalBytes - this.wroteBytes;
            if (rest < 0) {
                //no bug never here
                throw new IllegalStateException();
            }
            return rest;
        }

        private boolean writeBytes(final int length) {
            long wroteBytes = this.wroteBytes;
            wroteBytes += length;
            this.wroteBytes = wroteBytes;
            if (wroteBytes > this.totalBytes) {
                throw new IllegalArgumentException("length error");
            }
            return wroteBytes == this.totalBytes;
        }


    }// BigColumn


}
