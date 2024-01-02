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

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.util.MySQLCollections;
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
import io.netty.buffer.Unpooled;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

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
 * <br/>
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * <br/>
 *
 * @since 1.0
 */
abstract class MySQLResultSetReader implements ResultSetReader {

    private static final Path TEMP_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"), "jdbd/mysql/big_row")
            .toAbsolutePath();


    private static final String BIG_COLUMN_FILE_PREFIX = "big_column";

    private static final String BIG_COLUMN_FILE_SUFFIX = ".jdbd";

    private static final Logger LOG = LoggerFactory.getLogger(MySQLResultSetReader.class);


    static final Object MORE_CUMULATE_OBJECT = States.MORE_CUMULATE;

    final TaskAdjutant adjutant;

    final StmtTask task;

    final int capability;

    final FixedEnv fixedEnv;

    final Environment env;

    Charset resultSetCharset;

    private Throwable columnHandleError;

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
        if (currentRow == null) {
            return States.MORE_CUMULATE;
        }
        final ByteBuf oldBigPayload = this.bigPayload;
        final int oldReaderIndex = cumulateBuffer.readerIndex();
        States states;
        try {
            states = readRowSet(cumulateBuffer, serverStatesConsumer);
        } catch (Throwable e) {
            this.task.addErrorToTask(e); // add error , will skip all row packets
            LOG.debug("occur error,will skip all row packets", e);
            final ByteBuf newBigPayload = this.bigPayload;
            if (newBigPayload != null && newBigPayload != oldBigPayload) {
                if (newBigPayload.refCnt() > 0) {
                    newBigPayload.release();
                    LOG.debug("occur error,release new big payload");
                }
            }
            this.bigPayload = oldBigPayload;
            cumulateBuffer.readerIndex(oldReaderIndex);
            states = readRowSet(cumulateBuffer, serverStatesConsumer); // skip all row packets
        }
        return states;
    }


    /*################################## blow packet template method ##################################*/


    @Nullable
    abstract MySQLMutableCurrentRow readRowMeta(ByteBuf cumulateBuffer, Consumer<Object> serverStatesConsumer);


    abstract boolean readOneRow(ByteBuf cumulateBuffer, final boolean bigPayload, MySQLMutableCurrentRow currentRow);





    /*################################## blow final packet method ##################################*/


    final Charset columnCharset(final MySQLColumnMeta meta) {
        Charset charset = this.resultSetCharset;
        if (charset == null) {
            charset = meta.columnCharset;
        }
        return charset;
    }


    /**
     * @return <ul>
     * <li>{@link #MORE_CUMULATE_OBJECT} : more cumulate</li>
     * <li>column value</li>
     * </ul>
     */
    final Object readLongTextOrBlob(final ByteBuf payload, final MySQLColumnMeta meta,
                                    final MySQLMutableCurrentRow currentRow) {
        BigColumn bigColumn = currentRow.bigColumn;
        final Object value;
        final int readableBytes;
        readableBytes = payload.readableBytes();

        final long lenEnc;
        final byte[] bytes;
        if (bigColumn != null) {
            assert bigColumn.columnIndex == meta.columnIndex : "big column index not match";
            if (!readBigColumn(payload, bigColumn)) {
                value = MORE_CUMULATE_OBJECT;
            } else {
                currentRow.bigColumn = null; // clear big column
                switch (meta.sqlType) {
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                    case JSON:
                        value = TextPath.from(true, columnCharset(meta), bigColumn.path);
                        break;
                    case TINYBLOB:
                    case BLOB:
                    case MEDIUMBLOB:
                    case LONGBLOB:
                    case UNKNOWN:
                    default:
                        value = BlobPath.from(true, bigColumn.path);

                } // switch
            }
        } else if (readableBytes == 0) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((lenEnc = Packets.getLenEnc(payload, payload.readerIndex())) <= readableBytes) {
            assert Packets.readLenEnc(payload) == lenEnc : "getLenEnc bug";
            bytes = new byte[(int) lenEnc];
            payload.readBytes(bytes);
            switch (meta.sqlType) {
                case TINYTEXT:
                case TEXT:
                case MEDIUMTEXT:
                case LONGTEXT:
                case JSON:
                    value = new String(bytes, columnCharset(meta));
                    break;
                case TINYBLOB:
                case BLOB:
                case MEDIUMBLOB:
                case LONGBLOB:
                case UNKNOWN:
                default:
                    value = bytes;

            } // switch

        } else if (lenEnc <= this.fixedEnv.bigColumnBoundaryBytes) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((bigColumn = createBigColumnFile(meta.columnIndex, lenEnc)) == null) {
            payload.skipBytes(payload.readableBytes());
            value = MORE_CUMULATE_OBJECT;
        } else {
            assert Packets.readLenEnc(payload) == lenEnc : "getLenEnc bug";
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
            assert bigColumn.columnIndex == meta.columnIndex : "big column index not match";
            if (readBigColumn(payload, bigColumn)) {
                currentRow.bigColumn = null; // clear big column
                value = BlobPath.from(true, bigColumn.path);
            } else {
                value = MORE_CUMULATE_OBJECT;
            }
        } else if (readableBytes < 5) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((lenEnc = Packets.getLenEnc(payload, payload.readerIndex())) <= readableBytes) {
            assert Packets.readLenEnc(payload) == lenEnc : "getLenEnc bug";
            payload.skipBytes(4);// skip geometry prefix
            bytes = new byte[((int) lenEnc) - 4];
            payload.readBytes(bytes);
            value = bytes;
        } else if (lenEnc < this.fixedEnv.bigColumnBoundaryBytes) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((bigColumn = createBigColumnFile(meta.columnIndex, lenEnc - 4)) == null) {
            payload.skipBytes(payload.readableBytes()); // occur error ,skip
            value = MORE_CUMULATE_OBJECT;
        } else {
            assert Packets.readLenEnc(payload) == lenEnc : "getLenEnc bug";
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
                this.columnHandleError = error;
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
            if (Files.notExists(TEMP_DIRECTORY)) {
                Files.createDirectories(TEMP_DIRECTORY);
            }
            path = Files.createTempFile(TEMP_DIRECTORY, BIG_COLUMN_FILE_PREFIX, BIG_COLUMN_FILE_SUFFIX);
            this.task.addBigColumnPath(path);
            LOG.debug("create big column temp file complete,expected total size : {} about {} MB , {}",
                    totalBytes, totalBytes >> 20, path);
        } catch (Throwable e) {
            path = null;
            this.columnHandleError = e;
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
            this.columnHandleError = error = e;
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
        boolean oneRowEnd, cancelled;
        cancelled = task.isCancelled();

        outerLoop:
        for (int payloadLength, payloadIndex, writerIndex = 0, limitIndex; Packets.hasOnePacket(cumulateBuffer); ) {
            payloadLength = Packets.readInt3(cumulateBuffer);
            sequenceId = Packets.readInt1AsInt(cumulateBuffer);

            payloadIndex = cumulateBuffer.readerIndex();
            limitIndex = payloadIndex + payloadLength;

            if (bigPayload != null) {
                payload = bigPayload;
                if (cancelled) {
                    cumulateBuffer.skipBytes(payloadLength);
                } else {
                    payload.writeBytes(cumulateBuffer, payloadLength); // read current payload
                }
                if (payloadLength < Packets.MAX_PAYLOAD) {
                    this.bigPayload = bigPayload = null; // big row end
                    LOG.debug("big column payload end"); // bigColumn reset bytes 74 about 0 MB
                }
            } else switch (Packets.getInt1AsInt(cumulateBuffer, payloadIndex)) {
                case MySQLServerException.ERROR_HEADER: {
                    final MySQLServerException error;
                    error = MySQLServerException.read(cumulateBuffer, payloadLength, this.capability,
                            this.adjutant.errorCharset());
                    this.task.addErrorToTask(error);
                    LOG.debug("result set response error", error);
                    states = States.END_ON_ERROR;
                }
                break outerLoop;
                case EofPacket.EOF_HEADER: {
                    final Terminator terminator;
                    terminator = Terminator.fromCumulate(cumulateBuffer, payloadLength, this.capability);
                    serverStatesConsumer.accept(terminator);

                    if (!task.isCancelled()) {
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
                default: {
                    if (payloadLength < Packets.MAX_PAYLOAD) {
                        if (cancelled) {
                            cumulateBuffer.skipBytes(payloadLength);
                            continue;
                        }
                        writerIndex = cumulateBuffer.writerIndex();
                        if (limitIndex != writerIndex) {
                            cumulateBuffer.writerIndex(limitIndex);
                        }
                        payload = cumulateBuffer;
                    } else if (cancelled) {
                        cumulateBuffer.skipBytes(payloadLength);
                        this.bigPayload = bigPayload = Unpooled.EMPTY_BUFFER;
                        payload = bigPayload;
                    } else {
                        this.bigPayload = bigPayload = this.adjutant.allocator()
                                .directBuffer(payloadLength << 2, Integer.MAX_VALUE - 128);
                        payload = bigPayload;
                        payload.writeBytes(cumulateBuffer, payloadLength); // read current payload

                    } // else

                }// default

            }// switch

            if (cancelled) {
                continue;
            }

            oneRowEnd = readOneRow(payload, payload != cumulateBuffer, currentRow);  // read one row

            if (payload == cumulateBuffer) {
                assert oneRowEnd; // fail ,driver row end bug or server bug
                if (limitIndex != writerIndex) {
                    assert writerIndex > limitIndex; // fail , driver bug.
                    cumulateBuffer.writerIndex(writerIndex);
                }
                cumulateBuffer.readerIndex(limitIndex); //avoid tailor filler
            } else if (oneRowEnd) {
                assert bigPayload == null;
                payload.release();
            } else if (payload.readableBytes() == 0) {
                payload.clear();
            } else if (payload.readerIndex() > 0) {
                payload.discardReadBytes();
            }

            if (!oneRowEnd) {
                assert bigPayload != null;
                // MORE_CUMULATE
                break;
            }

            if (!(cancelled = this.columnHandleError != null)) {
                currentRow.rowCount++;
                task.next(currentRow);
                currentRow.resetCurrentRow();
            }
            if (!cancelled && ((currentRow.rowCount & 31) == 0) && this.task.isCancelled()) {
                cancelled = true;
            }

        } // outer loop

        if (sequenceId > -1) {
            task.updateSequenceId(sequenceId);
        }

        switch (states) {
            case END_ON_ERROR:
            case NO_MORE_RESULT:
            case MORE_RESULT: {
                // reset this instance
                bigPayload = this.bigPayload;
                if (bigPayload != null && bigPayload.refCnt() > 0) {
                    bigPayload.release();
                }
                this.currentRow = null;
                this.resultSetCharset = null;
                this.bigPayload = null;
                this.columnHandleError = null;
            }
            break;
            case MORE_CUMULATE:
            case MORE_FETCH:
            default:// no-op
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


    private static <T> Flux<T> setTypeToFlux(final ColumnMeta meta, final String source, final Class<T> elementClass) {

        Flux<T> flux;
        try {
            final Collection<T> collection;
            collection = parseSetString(meta, source, elementClass, MySQLCollections::arrayList);
            flux = Flux.fromIterable(collection);
        } catch (Throwable e) {
            flux = Flux.error(MySQLExceptions.wrap(e));
        }

        return flux;
    }

    /**
     * @throws IllegalArgumentException throw when {@link Enum#valueOf(Class, String)} throw
     */
    @SuppressWarnings("unchecked")
    private static <T, S extends Collection<T>> S parseSetString(final ColumnMeta meta, final String source,
                                                                 final Class<T> elementClass,
                                                                 final IntFunction<S> constructor)
            throws IllegalArgumentException {
        final Class<?> actualClass;
        if (Enum.class.isAssignableFrom(elementClass) && elementClass.isAnonymousClass()) {
            actualClass = elementClass.getSuperclass();
        } else {
            actualClass = elementClass;
        }
        final String[] elementArray;
        elementArray = source.split(",");
        final S collection = constructor.apply(elementArray.length);

        Enum<?> enumValue;
        for (String e : elementArray) {
            if (actualClass == String.class) {
                collection.add((T) e);
            } else {
                enumValue = ColumnConverts.convertToEnum(meta, actualClass, e);
                collection.add((T) enumValue);
            }
        }
        return collection;
    }


    static final class BigColumn {

        final int columnIndex;

        final Path path;

        private final long totalBytes;

        private final boolean debugEnabled;

        private long wroteBytes = 0L;

        private BigColumn(int columnIndex, Path path, long totalBytes) {
            this.columnIndex = columnIndex;
            this.path = path;
            this.totalBytes = totalBytes;
            this.debugEnabled = LOG.isDebugEnabled();
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

            if (this.debugEnabled) {
                final long reset;
                reset = restBytes();
                LOG.debug("bigColumn[{}] rest bytes {} about {} MB", this.columnIndex, reset, reset >> 20);
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
     * <br/>
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
            final Object value;
            if (columnClass == String.class && source instanceof byte[]) {
                if (meta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary) {
                    value = new String((byte[]) source, rowMeta.clientCharset);
                } else {
                    value = new String((byte[]) source, rowMeta.columnCharset(meta.columnCharset));
                }
            } else if (columnClass == String.class && source instanceof BlobPath) {
                try {
                    if (Files.size(((BlobPath) source).value()) > (Integer.MAX_VALUE - 128)) {
                        throw JdbdExceptions.cannotConvertColumnValue(meta, source, String.class, null);
                    }
                    final byte[] bytes;
                    bytes = Files.readAllBytes(((BlobPath) source).value());
                    value = new String(bytes, rowMeta.columnCharset(meta.columnCharset));
                } catch (Throwable e) {
                    throw JdbdExceptions.cannotConvertColumnValue(meta, source, String.class, e);
                }
            } else if (columnClass == Isolation.class) {
                value = toIsolation(meta, source);
            } else if (!(source instanceof Duration)) {
                value = ColumnConverts.convertToTarget(meta, source, columnClass, rowMeta.serverZone);
            } else if (columnClass == String.class) {
                value = MySQLTimes.durationToTimeText((Duration) source);
            } else {
                throw JdbdExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
            }
            return (T) value;
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

        @Override
        public final <T> Set<T> getSet(int indexBasedZero, Class<T> elementClass, IntFunction<Set<T>> constructor)
                throws JdbdException {
            final MySQLRowMeta rowMeta = this.rowMeta;

            final Object source;
            source = this.columnArray[rowMeta.checkIndex(indexBasedZero)];
            final MySQLColumnMeta meta = rowMeta.columnMetaArray[indexBasedZero];

            if (meta.sqlType != MySQLType.SET) {
                throw JdbdExceptions.cannotConvertElementColumnValue(meta, source, Set.class, elementClass, null);
            } else if (elementClass != String.class && !Enum.class.isAssignableFrom(elementClass)) {
                throw JdbdExceptions.cannotConvertElementColumnValue(meta, source, Set.class, elementClass, null);
            }

            if (source == null) {
                return constructor.apply(0);
            }

            try {
                return parseSetString(meta, (String) source, elementClass, constructor);
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
                case MEDIUMTEXT:
                case LONGTEXT:
                case JSON: {
                    if (valueClass != String.class) {
                        throw MySQLExceptions.cannotConvertElementColumnValue(meta, source, Publisher.class, valueClass, null);
                    } else if (source == null) {
                        publisher = Flux.empty();
                    } else if (source instanceof String) {
                        publisher = Flux.just((T) source);
                    } else if (source instanceof TextPath) {
                        publisher = (Flux<T>) ColumnConverts.convertToClobPublisher(meta, (TextPath) source, 2048);
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
                case MEDIUMBLOB:
                case GEOMETRY:
                case LONGBLOB: {
                    if (valueClass != byte[].class) {
                        throw MySQLExceptions.cannotConvertElementColumnValue(meta, source, Publisher.class, valueClass, null);
                    } else if (source == null) {
                        publisher = Flux.empty();
                    } else if (source instanceof byte[]) {
                        publisher = Flux.just((T) source);
                    } else if (source instanceof BlobPath) {
                        publisher = (Flux<T>) ColumnConverts.convertToBlobPublisher(meta, (BlobPath) source, 2048);
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
                    publisher = setTypeToFlux(meta, (String) source, valueClass);
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
     * <br/>
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
         * <br/>
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
