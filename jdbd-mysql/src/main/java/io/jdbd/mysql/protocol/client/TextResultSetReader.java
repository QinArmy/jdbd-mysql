package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.*;
import io.jdbd.result.ResultRow;
import io.jdbd.vendor.result.ErrorResultRow;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.type.LongStrings;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Consumer;

final class TextResultSetReader extends AbstractResultSetReader {

    private static final Logger LOG = LoggerFactory.getLogger(TextResultSetReader.class);


    TextResultSetReader(ResultSetReaderBuilder builder) {
        super(builder);
    }


    @Override
    boolean readResultSetMeta(final ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {

        final int negotiatedCapability = this.adjutant.negotiatedCapability();
        if ((negotiatedCapability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
            throw new IllegalStateException("Not support CLIENT_OPTIONAL_RESULTSET_METADATA");
        }
        boolean metaEnd;
        metaEnd = doReadRowMeta(cumulateBuffer);

        if (metaEnd && (negotiatedCapability & Capabilities.CLIENT_DEPRECATE_EOF) == 0) {
            if (Packets.hasOnePacket(cumulateBuffer)) {
                int payloadLength = Packets.readInt3(cumulateBuffer);
                updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
                EofPacket eof = EofPacket.read(cumulateBuffer.readSlice(payloadLength), negotiatedCapability);
                serverStatusConsumer.accept(eof);
            } else {
                metaEnd = false;
            }
        }
        return metaEnd;
    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }

    @Override
    int skipNullColumn(BigRowData bigRowData, final ByteBuf payload, final int columnIndex) {
        int i = columnIndex;
        final MySQLColumnMeta[] columnMetaArray = this.rowMeta.columnMetaArray;
        for (; i < columnMetaArray.length; i++) {
            if (Packets.getInt1AsInt(payload, payload.readerIndex()) != Packets.ENC_0) {
                break;
            }
            payload.readByte();
        }
        return i;
    }

    @Override
    ResultRow readOneRow(final ByteBuf payload) {
        final MySQLRowMeta rowMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta");
        final MySQLColumnMeta[] columnMetaArray = rowMeta.columnMetaArray;
        final Object[] rowValues = new Object[columnMetaArray.length];

        ResultRow resultRow;
        try {
            for (int i = 0; i < columnMetaArray.length; i++) {
                if (Packets.getInt1AsInt(payload, payload.readerIndex()) == Packets.ENC_0) {
                    payload.readByte();
                    continue;
                }
                rowValues[i] = readColumnValue(payload, columnMetaArray[i]);
            }
            resultRow = MySQLResultRow.from(rowValues, rowMeta, this.adjutant);
        } catch (Throwable e) {
            emitError(MySQLExceptions.wrap(e));
            resultRow = ErrorResultRow.INSTANCE;
        }
        return resultRow;
    }

    @Override
    long obtainColumnBytes(final MySQLColumnMeta columnMeta, final ByteBuf bigPayloadBuffer) {
        return Packets.getLenEncTotalByteLength(bigPayloadBuffer);
    }


    @Nullable
    @Override
    Object internalReadColumnValue(final ByteBuf payload, final MySQLColumnMeta columnMeta) {

        final Charset columnCharset = this.adjutant.obtainColumnCharset(columnMeta.columnCharset);

        String columnText;
        final Object columnValue;
        switch (columnMeta.mysqlType) {
            case CHAR:
            case VARCHAR:
            case JSON:
            case ENUM: {
                columnValue = Packets.readStringLenEnc(payload, columnCharset);
            }
            break;
            case SET: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = Collections.<String>emptySet();
                } else {
                    columnValue = MySQLStrings.spitAsSet(columnText, ",", true);
                }
            }
            break;
            case DECIMAL_UNSIGNED:
            case DECIMAL: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                columnValue = columnText == null ? null : new BigDecimal(columnText);
            }
            break;
            case BIGINT_UNSIGNED:
            case BIGINT: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnMeta.mysqlType == MySQLType.BIGINT_UNSIGNED) {
                    columnValue = new BigInteger(columnText);
                } else {
                    columnValue = Long.parseLong(columnText);
                }
            }
            break;
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT_UNSIGNED:
            case INT: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnMeta.mysqlType == MySQLType.INT_UNSIGNED) {
                    columnValue = Long.parseLong(columnText);
                } else {
                    columnValue = Integer.parseInt(columnText);
                }
            }
            break;
            case SMALLINT_UNSIGNED:
            case SMALLINT: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnMeta.mysqlType == MySQLType.SMALLINT_UNSIGNED) {
                    columnValue = Integer.parseInt(columnText);
                } else {
                    columnValue = Short.parseShort(columnText);
                }
            }
            break;
            case BOOLEAN: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else {
                    columnValue = MySQLConvertUtils.convertObjectToBoolean(columnText);
                }
            }
            break;
            case BIT: {
                if (columnMeta.isTiny1AsBit()) {
                    columnText = Packets.readStringLenEnc(payload, columnCharset);
                    if (columnText == null) {
                        columnValue = null;
                    } else {
                        columnValue = Byte.parseByte(columnText) == 0 ? 0L : 1L;
                    }
                } else {
                    byte[] bytes = Packets.readBytesLenEnc(payload);
                    if (bytes == null) {
                        columnValue = null;
                    } else {
                        columnValue = MySQLNumbers.readLongFromBigEndian(bytes, 0, bytes.length);
                    }
                }
            }
            break;
            case TINYINT_UNSIGNED:
            case TINYINT: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnMeta.mysqlType == MySQLType.TINYINT_UNSIGNED) {
                    columnValue = Short.parseShort(columnText);
                } else {
                    columnValue = Byte.parseByte(columnText);
                }
            }
            break;
            case DOUBLE_UNSIGNED:
            case DOUBLE: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else {
                    columnValue = Double.parseDouble(columnText);
                }
            }
            break;
            case FLOAT_UNSIGNED:
            case FLOAT: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else {
                    columnValue = Float.parseFloat(columnText);
                }
            }
            break;
            case DATE: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnText.equals("0000-00-00")) {
                    columnValue = handleZeroDateBehavior("DATE");
                } else {
                    columnValue = LocalDate.parse(columnText, DateTimeFormatter.ISO_LOCAL_DATE);
                }
            }
            break;
            case YEAR: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else {
                    columnValue = Year.of(Integer.parseInt(columnText));
                }
            }
            break;
            case TIMESTAMP:
            case DATETIME: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnText.startsWith("0000-00-00")) {
                    LocalDate date = handleZeroDateBehavior("DATETIME");
                    if (date == null) {
                        columnValue = null;
                    } else {
                        LocalDateTime dateTime;
                        String timeText = columnText.substring(10);
                        LocalTime time = LocalTime.parse(timeText, MySQLTimes.MYSQL_TIME_FORMATTER);
                        dateTime = LocalDateTime.of(date, time);
                        columnValue = dateTime;
                    }
                } else {
                    LocalDateTime dateTime = LocalDateTime.parse(columnText, MySQLTimes.MYSQL_DATETIME_FORMATTER);
                    columnValue = OffsetDateTime.of(dateTime, this.adjutant.obtainZoneOffsetDatabase())
                            .withOffsetSameInstant(this.adjutant.obtainZoneOffsetClient())
                            .toLocalDateTime();
                }
            }
            break;
            case TIME: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (MySQLTimes.isDuration(columnText)) {
                    columnValue = MySQLTimes.parseTimeAsDuration(columnText);
                } else {
                    LocalTime time = LocalTime.parse(columnText, MySQLTimes.MYSQL_TIME_FORMATTER);
                    columnValue = OffsetTime.of(time, this.adjutant.obtainZoneOffsetDatabase())
                            .withOffsetSameInstant(this.adjutant.obtainZoneOffsetClient())
                            .toLocalTime();
                }
            }
            break;
            case GEOMETRY: {
                final byte[] bytes = Packets.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    // drop MySQL internal 4 bytes for integer SRID
                    columnValue = LongBinaries.fromArray(Arrays.copyOfRange(bytes, 4, bytes.length));
                }
            }
            break;
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT: {
                columnText = Packets.readStringLenEnc(payload, columnCharset);
                if (columnText != null && columnMeta.mysqlType == MySQLType.LONGTEXT) {
                    columnValue = LongStrings.fromString(columnText);
                } else {
                    columnValue = columnText;
                }
            }
            break;
            case LONGBLOB: {
                final byte[] array = Packets.readBytesLenEnc(payload);
                if (array == null) {
                    columnValue = null;
                } else {
                    columnValue = LongBinaries.fromArray(array);
                }
            }
            break;
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case BINARY:
            case VARBINARY:
            case UNKNOWN:
            case NULL:
            default:
                // unknown
                columnValue = Packets.readBytesLenEnc(payload);

        }
        return columnValue;
    }

    @Override
    boolean isBinaryReader() {
        return false;
    }

    /*################################## blow private method ##################################*/


}
