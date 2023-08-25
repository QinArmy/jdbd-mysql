package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.BigColumnValue;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.session.Isolation;
import io.jdbd.type.BlobPath;
import io.jdbd.type.TextPath;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.result.ColumnConverts;
import io.jdbd.vendor.result.ColumnMeta;
import io.jdbd.vendor.result.VendorDataRow;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntFunction;

/**
 * <p>
 * This class is base class of following :
 *     <ul>
 *         <li>{@link TextResultSetReader}</li>
 *         <li>{@link BinaryResultSetReader}</li>
 *     </ul>
 * </p>
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
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

    Charset resultSetCharset;

    private Throwable error;

    private ByteBuf bigPayload;

    private MySQLMutableCurrentRow currentRow;


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
        MySQLMutableCurrentRow currentRow = this.currentRow;

        if (currentRow == null && (currentRow = readRowMeta(cumulateBuffer, serverStatesConsumer)) != null) {
            this.currentRow = currentRow;
            this.resultSetCharset = currentRow.rowMeta.resultSetCharset; // update
            if (currentRow.rowMeta.unknownCollationSet.size() > 0) {
                this.task.addErrorToTask(MySQLExceptions.unrecognizedCollationError(currentRow.rowMeta.unknownCollationSet));
            } else {
                this.task.next(currentRow.rowMeta); // emit ResultRowMeta as the header of query result.
            }
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
    abstract MySQLMutableCurrentRow readRowMeta(ByteBuf cumulateBuffer, Consumer<Object> serverStatesConsumer);


    abstract boolean readOneRow(ByteBuf cumulateBuffer, final boolean bigPayload, MySQLMutableCurrentRow currentRow);


    abstract Logger getLogger();


    /*################################## blow final packet method ##################################*/


    /**
     * @return <ul>
     * <li>{@link #MORE_CUMULATE_OBJECT} : more cumulate</li>
     * <li>column value</li>
     * </ul>
     */
    final Object readLongText(final ByteBuf payload, final MySQLColumnMeta meta,
                              final MySQLMutableCurrentRow currentRow) {
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
    final Object readLongBlob(final ByteBuf payload, final MySQLColumnMeta meta, final MySQLMutableCurrentRow currentRow) {
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
    final Object readGeometry(final ByteBuf payload, final MySQLColumnMeta meta, final MySQLMutableCurrentRow currentRow) {
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
        final MySQLMutableCurrentRow currentRow = this.currentRow;
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
                case MySQLServerException.ERROR_HEADER: {
                    sequenceId = Packets.readInt1AsInt(cumulateBuffer);
                    final MySQLServerException error;
                    error = MySQLServerException.read(cumulateBuffer, payloadLength, this.capability,
                            this.adjutant.errorCharset());
                    this.task.addErrorToTask(error);
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
                currentRow.resetCurrentRow();
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
            this.resultSetCharset = null;
            this.bigPayload = null;
            this.currentRow = null;
            this.error = null;
        }
        return states;
    }

    /*-------------------below static method -------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_transaction_isolation">transaction_isolation</a>
     */
    private static Isolation toIsolation(final MySQLColumnMeta meta, final Object source) {
        if (!(source instanceof String)) {
            throw JdbdExceptions.cannotConvertColumnValue(meta, source, Isolation.class, null);
        }
        final Isolation isolation;
        switch (((String) source).toUpperCase(Locale.ROOT)) {
            case "READ-COMMITTED":
                isolation = Isolation.READ_COMMITTED;
                break;
            case "REPEATABLE-READ":
                isolation = Isolation.REPEATABLE_READ;
                break;
            case "SERIALIZABLE":
                isolation = Isolation.SERIALIZABLE;
                break;
            case "READ-UNCOMMITTED":
                isolation = Isolation.READ_UNCOMMITTED;
                break;
            default:
                throw JdbdExceptions.cannotConvertColumnValue(meta, source, Isolation.class, null);
        }
        return isolation;
    }

    @SuppressWarnings("all")
    private static Flux<String> toStringFlux(final MySQLColumnMeta meta, final TextPath path) {
        return Flux.create(sink -> {
            final Charset charset;
            charset = path.charset();
            try (FileChannel channel = FileChannel.open(path.value(), MySQLBinds.openOptionSet(path))) {
                final ByteBuffer buffer = ByteBuffer.allocate(2048);

                for (int i = 0; channel.read(buffer) > 0; i++) {
                    buffer.flip();
                    sink.next(charset.decode(buffer).toString());
                    buffer.clear();

                    if ((i & 31) == 0 && sink.isCancelled()) {
                        break;
                    }
                }

                sink.complete();
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }

        });
    }

    @SuppressWarnings("all")
    private static Flux<byte[]> toBinaryFlux(final MySQLColumnMeta meta, final BlobPath path) {
        return Flux.create(sink -> {
            try (FileChannel channel = FileChannel.open(path.value(), MySQLBinds.openOptionSet(path))) {

                final ByteBuffer buffer = ByteBuffer.allocate(2048);
                byte[] dataBytes;
                for (int i = 0; channel.read(buffer) > 0; i++) {
                    buffer.flip();
                    dataBytes = new byte[buffer.remaining()];
                    buffer.get(dataBytes);
                    sink.next(dataBytes);
                    buffer.clear();

                    if ((i & 31) == 0 && sink.isCancelled()) {
                        break;
                    }
                }

                sink.complete();
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }

        });
    }

    @SuppressWarnings("unchecked")
    private static <T> Flux<T> setTypeToFlux(final String source, final Class<T> elementClass) {
        return Flux.create(sink -> {
            final Class<?> actualClass;
            if (Enum.class.isAssignableFrom(elementClass) && elementClass.isAnonymousClass()) {
                actualClass = elementClass.getSuperclass();
            } else {
                actualClass = elementClass;
            }
            try {

                final String[] elementArray;
                elementArray = source.split(",");
                Enum<?> enumValue;
                for (String e : elementArray) {
                    if (actualClass == String.class) {
                        sink.next((T) e);
                    } else {
                        enumValue = ColumnConverts.convertToEnum(actualClass, e);
                        sink.next((T) enumValue);
                    }
                }
                sink.complete();
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }

        });
    }


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

    /**
     * <p>
     * This class is base class of following :
     *     <ul>
     *         <li>{@link MySQLCurrentRow}</li>
     *         <li>{@link MySQLResultRow}</li>
     *     </ul>
     * </p>
     */
    abstract static class MySQLDataRow extends VendorDataRow {

        final MySQLRowMeta rowMeta;

        final Object[] columnArray;


        /**
         * private constructor
         */
        private MySQLDataRow(MySQLRowMeta rowMeta) {
            this.rowMeta = rowMeta;
            final int arrayLength;
            arrayLength = rowMeta.columnMetaArray.length;
            this.columnArray = new Object[arrayLength];
        }

        /**
         * private constructor
         */
        private MySQLDataRow(final MySQLCurrentRow currentRow) {
            this.rowMeta = currentRow.rowMeta;

            if (currentRow instanceof MySQLImmutableCurrentRow) { // immutable
                this.columnArray = currentRow.columnArray;
            } else {
                final Object[] columnArray = new Object[currentRow.columnArray.length];
                System.arraycopy(currentRow.columnArray, 0, columnArray, 0, columnArray.length);
                this.columnArray = columnArray;
            }
        }

        @Override
        public final int getResultNo() {
            return this.rowMeta.resultNo;
        }

        @Override
        public final ResultRowMeta getRowMeta() {
            return this.rowMeta;
        }

        @Override
        public final boolean isBigColumn(final int indexBasedZero) {
            return this.columnArray[this.rowMeta.checkIndex(indexBasedZero)] instanceof BigColumnValue;
        }

        @Override
        public final boolean isNull(int indexBasedZero) throws JdbdException {
            return this.columnArray[this.rowMeta.checkIndex(indexBasedZero)] == null;
        }

        @Override
        public final Object get(int indexBasedZero) throws JdbdException {
            return this.columnArray[this.rowMeta.checkIndex(indexBasedZero)];
        }

        @SuppressWarnings("unchecked")
        @Override
        public final <T> T get(final int indexBasedZero, final Class<T> columnClass) throws JdbdException {
            final MySQLRowMeta rowMeta = this.rowMeta;

            final Object source;
            source = this.columnArray[rowMeta.checkIndex(indexBasedZero)];

            if (source == null || columnClass.isInstance(source)) {
                return (T) source;
            }

            final MySQLColumnMeta meta = rowMeta.columnMetaArray[indexBasedZero];
            final T value;
            if (columnClass == Isolation.class) {
                value = (T) toIsolation(meta, source);
            } else if (!(source instanceof Duration)) {
                value = ColumnConverts.convertToTarget(meta, source, columnClass, rowMeta.serverZone);
            } else if (columnClass == String.class) {
                value = (T) MySQLTimes.durationToTimeText((Duration) source);
            } else {
                throw JdbdExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
            }
            return value;
        }


        @Override
        public final <T> List<T> getList(int indexBasedZero, Class<T> elementClass, IntFunction<List<T>> constructor)
                throws JdbdException {
            final MySQLRowMeta rowMeta = this.rowMeta;

            final Object source;
            source = this.columnArray[rowMeta.checkIndex(indexBasedZero)];
            final MySQLColumnMeta meta = rowMeta.columnMetaArray[indexBasedZero];
            //TODO GEOMETRY
            throw JdbdExceptions.cannotConvertColumnValue(meta, source, List.class, null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public final <T> Set<T> getSet(int indexBasedZero, Class<T> elementClass, IntFunction<Set<T>> constructor)
                throws JdbdException {
            final MySQLRowMeta rowMeta = this.rowMeta;

            final Object source;
            source = this.columnArray[rowMeta.checkIndex(indexBasedZero)];
            final MySQLColumnMeta meta = rowMeta.columnMetaArray[indexBasedZero];

            if (meta.sqlType != MySQLType.SET) {
                throw JdbdExceptions.cannotConvertElementColumnValue(meta, source, Set.class, elementClass, null);
            }

            if (source == null) {
                return Collections.emptySet();
            }

            final Class<?> actualClass;
            if (elementClass == String.class) {
                actualClass = elementClass;
            } else if (!Enum.class.isAssignableFrom(elementClass)) {
                throw JdbdExceptions.cannotConvertElementColumnValue(meta, source, Set.class, elementClass, null);
            } else if (elementClass.isAnonymousClass()) {
                actualClass = elementClass.getSuperclass();
            } else {
                actualClass = elementClass;
            }

            try {

                final String[] elementArray;
                elementArray = ((String) source).split(",");

                final Set<T> set = constructor.apply((int) (elementArray.length / 0.75f));
                Enum<?> enumValue;
                for (String e : elementArray) {
                    if (actualClass == String.class) {
                        set.add((T) e);
                    } else {
                        enumValue = ColumnConverts.convertToEnum(actualClass, e);
                        set.add((T) enumValue);
                    }
                }

                return set;
            } catch (Throwable e) {
                throw JdbdExceptions.cannotConvertElementColumnValue(meta, source, Set.class, elementClass, e);
            }

        }

        @Override
        public final <K, V> Map<K, V> getMap(int indexBasedZero, Class<K> keyClass, Class<V> valueClass,
                                             IntFunction<Map<K, V>> constructor) throws JdbdException {
            final MySQLRowMeta rowMeta = this.rowMeta;

            final Object source;
            source = this.columnArray[rowMeta.checkIndex(indexBasedZero)];
            final MySQLColumnMeta meta = rowMeta.columnMetaArray[indexBasedZero];

            throw JdbdExceptions.cannotConvertColumnValue(meta, source, Map.class, null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public final <T> Publisher<T> getPublisher(final int indexBasedZero, final Class<T> valueClass)
                throws JdbdException {

            final MySQLRowMeta rowMeta = this.rowMeta;

            final Object source;
            source = this.columnArray[rowMeta.checkIndex(indexBasedZero)];
            final MySQLColumnMeta meta = rowMeta.columnMetaArray[indexBasedZero];

            final Publisher<T> publisher;
            switch (meta.sqlType) {
                case CHAR:
                case VARCHAR:
                case TINYTEXT:
                case TEXT:
                case MEDIUMTEXT: {
                    if (source == null) {
                        publisher = Flux.empty();
                    } else if (valueClass == String.class && source instanceof String) {
                        publisher = Flux.just((T) source);
                    } else {
                        throw MySQLExceptions.cannotConvertElementColumnValue(meta, source, Publisher.class, valueClass, null);
                    }
                }
                break;
                case LONGTEXT:
                case JSON: {
                    if (source == null) {
                        publisher = Flux.empty();
                    } else if (valueClass != String.class) {
                        throw MySQLExceptions.cannotConvertElementColumnValue(meta, source, Publisher.class, valueClass, null);
                    } else if (source instanceof String) {
                        publisher = Flux.just((T) source);
                    } else if (source instanceof TextPath) {
                        publisher = (Flux<T>) toStringFlux(meta, (TextPath) source);
                    } else {
                        // no bug,never here
                        throw MySQLExceptions.cannotConvertElementColumnValue(meta, source, Publisher.class, valueClass, null);
                    }
                }
                break;
                case BINARY:
                case VARBINARY:
                case TINYBLOB:
                case BLOB:
                case MEDIUMBLOB: {
                    if (source == null) {
                        publisher = Flux.empty();
                    } else if (valueClass == byte[].class && source instanceof byte[]) {
                        publisher = Flux.just((T) source);
                    } else {
                        throw MySQLExceptions.cannotConvertElementColumnValue(meta, source, Publisher.class, valueClass, null);
                    }
                }
                break;
                case GEOMETRY:
                case LONGBLOB: {
                    if (source == null) {
                        publisher = Flux.empty();
                    } else if (valueClass != byte[].class) {
                        throw MySQLExceptions.cannotConvertElementColumnValue(meta, source, Publisher.class, valueClass, null);
                    } else if (source instanceof byte[]) {
                        publisher = Flux.just((T) source);
                    } else if (source instanceof BlobPath) {
                        publisher = (Flux<T>) toBinaryFlux(meta, (BlobPath) source);
                    } else {
                        // no bug,never here
                        throw MySQLExceptions.cannotConvertElementColumnValue(meta, source, Publisher.class, valueClass, null);
                    }
                }
                break;
                case SET: {
                    if (valueClass != String.class && !Enum.class.isAssignableFrom(valueClass)) {
                        throw MySQLExceptions.cannotConvertElementColumnValue(meta, source, Publisher.class, valueClass, null);
                    }
                    publisher = setTypeToFlux((String) source, valueClass);
                }
                break;
                default:
                    throw MySQLExceptions.cannotConvertElementColumnValue(meta, source, Publisher.class, valueClass, null);
            }
            return publisher;
        }


        @Override
        protected final ColumnMeta getColumnMeta(final int safeIndex) {
            return this.rowMeta.columnMetaArray[safeIndex];
        }


    }// MySQLDataRow


    /**
     * <p>
     * This class is base class of following :
     *     <ul>
     *         <li>{@link MySQLMutableCurrentRow}</li>
     *         <li>{@link MySQLImmutableCurrentRow}</li>
     *     </ul>
     * </p>
     */
    private static abstract class MySQLCurrentRow extends MySQLDataRow implements CurrentRow {

        /**
         * private constructor
         */
        private MySQLCurrentRow(MySQLRowMeta rowMeta) {
            super(rowMeta);
        }

        /**
         * private constructor
         */
        private MySQLCurrentRow(MySQLMutableCurrentRow currentRow) {
            super(currentRow);
        }

        @Override
        public final ResultRow asResultRow() {
            return new MySQLResultRow(this);
        }


    }//MySQLCurrentRow


    static abstract class MySQLMutableCurrentRow extends MySQLCurrentRow {

        private BigColumn bigColumn;

        private long rowCount = 0L;
        private boolean bigRow;

        /**
         * <p>
         * package constructor for following :
         *     <ul>
         *         <li>{@link TextResultSetReader.TextMutableCurrentRow}</li>
         *         <li>{@link BinaryResultSetReader.BinaryMutableCurrentRow}</li>
         *     </ul>
         * </p>
         */
        MySQLMutableCurrentRow(MySQLRowMeta rowMeta) {
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

        @Override
        protected final CurrentRow copyCurrentRowIfNeed() {
            return new MySQLImmutableCurrentRow(this);
        }

        final void setBigColumn(BigColumn bigColumn) {
            if (this.bigColumn != null) {
                throw new IllegalStateException();
            }
            this.bigColumn = bigColumn;
            this.bigRow = true;
        }

        abstract void reset();

        private void resetCurrentRow() {
            bigRow = false;
            this.reset();
        }


    }//MySQLMutableCurrentRow

    private static final class MySQLImmutableCurrentRow extends MySQLCurrentRow {

        private final long rowNumber;

        private final boolean bigRow;

        private MySQLImmutableCurrentRow(MySQLMutableCurrentRow currentRow) {
            super(currentRow);
            this.rowNumber = currentRow.rowCount;
            this.bigRow = currentRow.bigRow;
        }

        @Override
        public long rowNumber() {
            return this.rowNumber;
        }

        @Override
        public boolean isBigRow() {
            return this.bigRow;
        }

        @Override
        protected CurrentRow copyCurrentRowIfNeed() {
            return this;
        }


    }//MySQLImmutableCurrentRow

    /**
     * private class
     *
     * @see MySQLCurrentRow#asResultRow()
     */
    private static final class MySQLResultRow extends MySQLDataRow implements ResultRow {

        private final boolean bigRow;

        /**
         * private constructor
         *
         * @see MySQLCurrentRow#asResultRow()
         */
        private MySQLResultRow(MySQLCurrentRow currentRow) {
            super(currentRow);
            this.bigRow = currentRow.isBigRow();
        }

        @Override
        public boolean isBigRow() {
            return this.bigRow;
        }


    }//MySQLResultRow


}
