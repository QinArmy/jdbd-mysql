package io.jdbd.mysql.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.util.MySQLExceptions;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.*;
import java.util.function.Consumer;


/**
 * <p>
 * This class is the implementation reader of MySQL binary ResultSet .
 * </p>
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see ComPreparedTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html">Binary Protocol Resultset</a>
 */
final class BinaryResultSetReader extends MySQLResultSetReader {

    static BinaryResultSetReader create(StmtTask task) {
        return new BinaryResultSetReader(task);
    }

    private static final Logger LOG = LoggerFactory.getLogger(BinaryResultSetReader.class);

    private static final byte BINARY_ROW_HEADER = 0x00;


    private BinaryResultSetReader(StmtTask task) {
        super(task);
    }


    @Override
    MySQLCurrentRow readRowMeta(ByteBuf cumulateBuffer, Consumer<Object> serverStatesConsumer) {
        if (!MySQLRowMeta.canReadMeta(cumulateBuffer, false)) {
            return null;
        }
        return new BinaryCurrentRow(MySQLRowMeta.readForRows(cumulateBuffer, this.task));
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html">Binary Protocol Resultset</a>
     */
    @Override
    boolean readOneRow(final ByteBuf payload, final boolean bigPayload, final MySQLCurrentRow currentRow) {
        final MySQLColumnMeta[] metaArray = currentRow.rowMeta.columnMetaArray;
        if (payload.readByte() != BINARY_ROW_HEADER) {
            throw new IllegalArgumentException("cumulateBuffer isn't binary row");
        }
        final BinaryCurrentRow binaryCurrentRow = (BinaryCurrentRow) currentRow;
        final Object[] columnValues = currentRow.columnArray;
        final byte[] nullBitMap = binaryCurrentRow.nullBitMap;

        MySQLColumnMeta columnMeta;
        int columnIndex = binaryCurrentRow.columnIndex;

        if (columnIndex == 0 && binaryCurrentRow.readNullBitMap) {
            payload.readBytes(nullBitMap); // null_bitmap
            binaryCurrentRow.readNullBitMap = false; // avoid first column is big column
        }
        Object value;
        boolean moreCumulate = false;
        for (int byteIndex, bitIndex; columnIndex < metaArray.length; columnIndex++) {
            columnMeta = metaArray[columnIndex];
            byteIndex = (columnIndex + 2) & (~7);
            bitIndex = (columnIndex + 2) & 7;
            if ((nullBitMap[byteIndex] & (1 << bitIndex)) != 0) {
                columnValues[columnIndex] = null;
                continue;
            }
            value = readOneColumn(payload, bigPayload, columnMeta, binaryCurrentRow);
            if (value == MORE_CUMULATE_OBJECT) {
                moreCumulate = true;
                break;
            }
            columnValues[columnIndex] = value;
        }
        binaryCurrentRow.columnIndex = columnIndex;
        return !moreCumulate && columnIndex == metaArray.length;
    }


    @Override
    Logger getLogger() {
        return LOG;
    }


    /**
     * @return one of <ul>
     * <li>null : {@code DATETIME} is zero.</li>
     * <li>{@link #MORE_CUMULATE_OBJECT} more cumulate</li>
     * <li>column value</li>
     * </ul>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html">Binary Protocol Resultset</a>
     */
    @SuppressWarnings("deprecation")
    @Nullable
    private Object readOneColumn(final ByteBuf payload, final boolean bigPayload, final MySQLColumnMeta meta,
                                 final BinaryCurrentRow currentRow) {
        final int readableBytes;
        if (bigPayload) {
            readableBytes = payload.readableBytes();
        } else {
            readableBytes = Integer.MAX_VALUE;
        }
        final Object value;
        final byte[] bytes;
        switch (meta.sqlType) {
            case TINYINT: {
                if (readableBytes > 0) {
                    value = payload.readByte();
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case TINYINT_UNSIGNED: {
                if (readableBytes > 0) {
                    value = (short) (payload.readByte() & 0xFF);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case SMALLINT: {
                if (readableBytes > 1) {
                    value = (short) Packets.readInt2AsInt(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case SMALLINT_UNSIGNED: {
                if (readableBytes > 1) {
                    value = Packets.readInt2AsInt(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case MEDIUMINT: {
                final int v;
                if (readableBytes < 3) {
                    value = MORE_CUMULATE_OBJECT;
                } else if (((v = Packets.readInt3(payload)) & 0x80_00_00) == 0) {//positive
                    value = v;
                } else {//negative
                    final int complement = (v & 0x7F_FF_FF);
                    if (complement == 0) {
                        value = (-(0x7F_FF_FF) - 1);
                    } else {
                        value = -((complement - 1) ^ 0x7F_FF_FF);
                    }
                }
            }
            break;
            case MEDIUMINT_UNSIGNED: {
                if (readableBytes > 2) {
                    value = Packets.readInt3(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case INT: {
                if (readableBytes > 3) {
                    value = Packets.readInt4(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case INT_UNSIGNED: {
                if (readableBytes > 3) {
                    value = Packets.readInt4AsLong(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case BIGINT: {
                if (readableBytes > 7) {
                    value = Packets.readInt8(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case BIGINT_UNSIGNED: {
                if (readableBytes > 7) {
                    value = Packets.readInt8AsBigInteger(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case DECIMAL:
            case DECIMAL_UNSIGNED: {
                final int lenEnc;
                if (readableBytes == 0 || (lenEnc = Packets.readLenEncAsInt(payload)) > readableBytes) {
                    value = MORE_CUMULATE_OBJECT;
                } else {
                    bytes = new byte[lenEnc];
                    payload.readBytes(bytes);
                    value = new BigDecimal(new String(bytes, this.adjutant.columnCharset(meta.columnCharset)));
                }
            }
            break;
            case FLOAT:
            case FLOAT_UNSIGNED: {
                if (readableBytes > 3) {
                    value = Float.intBitsToFloat(Packets.readInt4(payload));
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case DOUBLE:
            case DOUBLE_UNSIGNED: {
                if (readableBytes > 7) {
                    value = Double.longBitsToDouble(Packets.readInt8(payload));
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case BOOLEAN: {
                if (readableBytes > 0) {
                    value = payload.readByte() != 0;
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case BIT:
                value = readBitType(payload, readableBytes);
                break;
            case TIME:
                value = readBinaryTimeType(payload, readableBytes);
                break;
            case DATE:
                value = readBinaryDate(payload, readableBytes);
                break;
            case YEAR: {
                if (readableBytes > 1) {
                    value = Year.of(Packets.readInt2AsInt(payload));
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case TIMESTAMP:
            case DATETIME: {
                if (readableBytes == 0 || payload.getByte(payload.readerIndex()) > readableBytes) {
                    value = MORE_CUMULATE_OBJECT;
                } else {
                    value = readBinaryDateTime(payload);
                }
            }
            break;
            case CHAR:
            case VARCHAR:
            case ENUM:
            case SET:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT: {
                final int lenEnc;
                if (readableBytes == 0 || (lenEnc = Packets.readLenEncAsInt(payload)) > readableBytes) {
                    value = MORE_CUMULATE_OBJECT;
                } else {
                    bytes = new byte[lenEnc];
                    payload.readBytes(bytes);
                    value = new String(bytes, this.adjutant.columnCharset(meta.columnCharset));
                }
            }
            break;
            case LONGTEXT:
            case JSON: // handle JSON as long text
                value = readLongText(payload, meta, currentRow);
                break;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB: {
                final int lenEnc;
                if (readableBytes == 0 || (lenEnc = Packets.readLenEncAsInt(payload)) > readableBytes) {
                    value = MORE_CUMULATE_OBJECT;
                } else {
                    bytes = new byte[lenEnc];
                    payload.readBytes(bytes);
                    value = bytes;
                }
            }
            break;
            case GEOMETRY:
                value = readGeometry(payload, meta, currentRow);
                break;
            case NULL:
                throw MySQLExceptions.createFatalIoException("server return null type", null);
            case LONGBLOB:
            case UNKNOWN:// handle unknown as long blob.
            default:
                value = readLongBlob(payload, meta, currentRow);

        } //switch
        return value;
    }



    /*################################## blow private method ##################################*/


    /**
     * @return {@link LocalTime} or {@link Duration}
     * @see #readOneColumn(ByteBuf, boolean, MySQLColumnMeta, BinaryCurrentRow)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_time">ProtocolBinary::MYSQL_TYPE_TIME</a>
     */
    private Object readBinaryTimeType(final ByteBuf byteBuf, final int readableBytes) {
        if (readableBytes == 0) {
            return MORE_CUMULATE_OBJECT;
        }
        final int length = byteBuf.readByte();
        if (length == 0) {
            return LocalTime.MIDNIGHT;
        } else if (length > readableBytes) {
            byteBuf.readerIndex(byteBuf.readerIndex() - 1);
            return MORE_CUMULATE_OBJECT;
        }
        final Object value;
        final boolean negative = byteBuf.readByte() == 1;
        final int days, hours, minutes, seconds;
        days = Packets.readInt4(byteBuf);
        hours = byteBuf.readByte();
        minutes = byteBuf.readByte();
        seconds = byteBuf.readByte();

        if (negative || days != 0 || hours > 23) {
            int totalSeconds = (days * 24 * 3600) + (hours * 3600) + (minutes * 60) + seconds;
            if (negative) {
                totalSeconds = -totalSeconds;
            }
            if (length == 8) {
                value = Duration.ofSeconds(totalSeconds);
            } else {
                value = Duration.ofSeconds(totalSeconds, Packets.readInt4(byteBuf) * 1000L);
            }
        } else if (length == 8) {
            value = LocalTime.of(hours, minutes, seconds);
        } else {
            value = LocalTime.of(hours, minutes, seconds, Packets.readInt4(byteBuf) * 1000);
        }
        return value;
    }


    /**
     * @see #readOneColumn(ByteBuf, boolean, MySQLColumnMeta, BinaryCurrentRow)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_date">ProtocolBinary::MYSQL_TYPE_DATETIME</a>
     */
    @Nullable
    private LocalDateTime readBinaryDateTime(final ByteBuf payload) {

        final int length = payload.readByte();
        LocalDateTime dateTime;
        switch (length) {
            case 7: {
                dateTime = LocalDateTime.of(
                        Packets.readInt2AsInt(payload), // year
                        payload.readByte(),        // month
                        payload.readByte(),        // day

                        payload.readByte(),         // hour
                        payload.readByte(),         // minute
                        payload.readByte()          // second
                );
            }
            break;
            case 11: {
                dateTime = LocalDateTime.of(
                        Packets.readInt2AsInt(payload), // year
                        payload.readByte(),         // month
                        payload.readByte(),          // day

                        payload.readByte(),          // hour
                        payload.readByte(),         // minute
                        payload.readByte(),        // second

                        Packets.readInt4(payload) * 1000 // micro second
                );
            }
            break;
            case 4: {
                LocalDate date = LocalDate.of(Packets.readInt2AsInt(payload), payload.readByte(), payload.readByte());
                dateTime = LocalDateTime.of(date, LocalTime.MIN);
            }
            break;
            case 0: {
                dateTime = null;
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException(
                        String.format("Server send binary MYSQL_TYPE_DATETIME length[%s] error.", length), null);
        }

        if (dateTime == null) {
            LocalDate date = handleZeroDateBehavior("DATETIME");
            if (date != null) {
                dateTime = LocalDateTime.of(date, LocalTime.MIDNIGHT);
            }
        }
        return dateTime;
    }

    /**
     * @see #readOneColumn(ByteBuf, boolean, MySQLColumnMeta, BinaryCurrentRow)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_date">ProtocolBinary::MYSQL_TYPE_DATE</a>
     */
    @Nullable
    private Object readBinaryDate(final ByteBuf payload, final int readableBytes) {
        if (readableBytes == 0) {
            return MORE_CUMULATE_OBJECT;
        }
        final int length = payload.readByte();
        if (length > readableBytes) {
            payload.readerIndex(payload.readerIndex() - 1);
            return MORE_CUMULATE_OBJECT;
        }
        LocalDate date;
        switch (length) {
            case 4:
                date = LocalDate.of(Packets.readInt2AsInt(payload), payload.readByte(), payload.readByte());
                break;
            case 0:
                date = null;
                break;
            default:
                throw MySQLExceptions.createFatalIoException(
                        String.format("Server send binary MYSQL_TYPE_DATE length[%s] error.", length), null);
        }
        if (date == null) {
            // maybe null
            date = handleZeroDateBehavior("DATE");
        }
        return date;
    }

    /*################################## blow static method ##################################*/

    /*################################## blow private static method ##################################*/

    private static MySQLJdbdException createResponseNullBinaryError(MySQLColumnMeta meta) {
        String m = String.format("MySQL server response null value in binary protocol,meta:%s", meta);
        return new MySQLJdbdException(m);
    }


    private static final class BinaryCurrentRow extends MySQLCurrentRow {


        private final byte[] nullBitMap;

        private int columnIndex = 0;

        private boolean readNullBitMap = true;


        private BinaryCurrentRow(MySQLRowMeta rowMeta) {
            super(rowMeta);
            this.nullBitMap = new byte[(rowMeta.columnMetaArray.length + 9) >> 3];
        }


        @Override
        void doRest() {
            if (this.columnIndex != this.columnArray.length) {
                throw new IllegalStateException();
            }
            this.readNullBitMap = true;
            this.columnIndex = 0;
        }

    }//BinaryCurrentRow


}
