package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLTimes;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.function.Consumer;


/**
 * <p>
 * This class is the implementation reader of MySQL Text ResultSet .
 * </p>
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see ComQueryTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html">Text Resultset</a>
 */
final class TextResultSetReader extends MySQLResultSetReader {

    static TextResultSetReader create(StmtTask task) {
        return new TextResultSetReader(task);
    }

    private static final Logger LOG = LoggerFactory.getLogger(TextResultSetReader.class);

    private TextResultSetReader(StmtTask task) {
        super(task);
    }


    @Override
    MySQLCurrentRow readRowMeta(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer) {
        if ((this.capability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
            throw new IllegalStateException("Not support CLIENT_OPTIONAL_RESULTSET_METADATA");
        }
        final boolean eofEnd = (this.capability & Capabilities.CLIENT_DEPRECATE_EOF) == 0;

        if (!MySQLRowMeta.canReadMeta(cumulateBuffer, eofEnd)) {
            return null;
        }
        final TextCurrentRow currentRow;
        currentRow = new TextCurrentRow(MySQLRowMeta.readForRows(cumulateBuffer, this.task));
        if (eofEnd) {
            final int payloadLength;
            payloadLength = Packets.readInt3(cumulateBuffer);
            this.task.updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
            serverStatesConsumer.accept(EofPacket.readCumulate(cumulateBuffer, payloadLength, this.capability));
        }
        return currentRow;
    }

    @Override
    boolean readOneRow(final ByteBuf cumulateBuffer, final boolean bigPayload, final MySQLCurrentRow currentRow) {
        final MySQLColumnMeta[] columnMetaArray = currentRow.rowMeta.columnMetaArray;
        final Object[] columnValues = currentRow.columnArray;

        final TextCurrentRow textCurrentRow = (TextCurrentRow) currentRow;
        final boolean[] firstBits = textCurrentRow.firstBits;
        int columnIndex = textCurrentRow.columnIndex;
        Object value;
        boolean moreCumulate = false;
        for (; columnIndex < columnMetaArray.length; columnIndex++) {

            if (firstBits[columnIndex]) {
                firstBits[columnIndex] = false;
                if (Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex()) == Packets.ENC_0) {
                    cumulateBuffer.readByte();
                    columnValues[columnIndex] = null;
                    continue;
                }
            }
            value = readOneColumn(cumulateBuffer, bigPayload, columnMetaArray[columnIndex], textCurrentRow);
            if (value == MORE_CUMULATE_OBJECT) {
                moreCumulate = true;
                break;
            }
            columnValues[columnIndex] = value;
        }

        textCurrentRow.columnIndex = columnIndex;

        return !moreCumulate && columnIndex == columnMetaArray.length;
    }


    @Override
    Logger getLogger() {
        return LOG;
    }


    @SuppressWarnings("deprecation")
    @Nullable
    private Object readOneColumn(final ByteBuf payload, final boolean bigPayload, final MySQLColumnMeta meta,
                                 final TextCurrentRow currentRow) {
        final int readableBytes;
        if (bigPayload) {
            readableBytes = payload.readableBytes();
        } else {
            readableBytes = Integer.MAX_VALUE;
        }
        String columnText;
        final Object bigColumnValue;
        final boolean bigColumn;
        switch (meta.sqlType) {
            case NULL:
                throw MySQLExceptions.createFatalIoException("server text protocol return null type", null);
            case LONGTEXT:
            case JSON:
                bigColumnValue = readLongText(payload, meta, currentRow);
                bigColumn = true;
                break;
            case GEOMETRY:
                bigColumnValue = readGeometry(payload, meta, currentRow);
                bigColumn = true;
                break;
            case LONGBLOB:
            case UNKNOWN:
                bigColumnValue = readLongBlob(payload, meta, currentRow);
                bigColumn = true;
                break;
            default:
                bigColumnValue = null;
                bigColumn = false;

        } // outer switch

        if (bigColumn) {
            return bigColumnValue;
        }

        final Object value;
        final byte[] bytes;
        final int lenEnc;
        if (readableBytes == 0 || (lenEnc = Packets.readLenEncAsInt(payload)) > readableBytes) {
            return MORE_CUMULATE_OBJECT;
        }
        bytes = new byte[lenEnc];
        payload.readBytes(bytes);

        final Charset columnCharset;
        columnCharset = this.adjutant.columnCharset(meta.columnCharset);

        switch (meta.sqlType) {
            case BOOLEAN:
                value = Byte.parseByte(new String(bytes, columnCharset)) != 0;
                break;
            case TINYINT:
                value = Byte.parseByte(new String(bytes, columnCharset));
                break;
            case TINYINT_UNSIGNED:
            case SMALLINT:
                value = Short.parseShort(new String(bytes, columnCharset));
                break;
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT:
                value = Integer.parseInt(new String(bytes, columnCharset));
                break;
            case INT_UNSIGNED:
            case BIGINT:
                value = Long.parseLong(new String(bytes, columnCharset));
                break;
            case BIGINT_UNSIGNED:
                value = new BigInteger(new String(bytes, columnCharset));
                break;
            case DECIMAL_UNSIGNED:
            case DECIMAL:
                value = new BigDecimal(new String(bytes, columnCharset));
                break;
            case FLOAT_UNSIGNED:
            case FLOAT:
                value = Float.parseFloat(new String(bytes, columnCharset));
                break;
            case DOUBLE_UNSIGNED:
            case DOUBLE:
                value = Double.parseDouble(new String(bytes, columnCharset));
                break;
            case CHAR:
            case VARCHAR:
            case ENUM:
            case SET:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
                value = new String(bytes, columnCharset);
                break;
            case BIT:
                value = parseBitAsLong(bytes);
                break;
            case TIME:
                value = LocalTime.parse(new String(bytes, columnCharset));
                break;
            case DATE: {
                columnText = new String(bytes, columnCharset);
                if (columnText.equals("0000-00-00")) {
                    value = handleZeroDateBehavior("DATE");
                } else {
                    value = LocalDate.parse(columnText, DateTimeFormatter.ISO_LOCAL_DATE);
                }
            }
            break;
            case YEAR:
                value = Year.of(Integer.parseInt(new String(bytes, columnCharset)));
                break;
            case TIMESTAMP:
            case DATETIME: {
                columnText = new String(bytes, columnCharset);
                final LocalDate date;
                if (!columnText.startsWith("0000-00-00")) {
                    value = LocalDateTime.parse(columnText, MySQLTimes.DATETIME_FORMATTER_6);
                } else if ((date = handleZeroDateBehavior("DATETIME")) == null) {
                    value = null;
                } else {
                    LocalTime time = LocalTime.parse(columnText.substring(11), MySQLTimes.TIME_FORMATTER_6);
                    value = LocalDateTime.of(date, time);
                }
            }
            break;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
                value = bytes;
                break;
            default:
                throw MySQLExceptions.unexpectedEnum(meta.sqlType);

        } // inter switch


        return value;
    }



    /*################################## blow private method ##################################*/


    private static final class TextCurrentRow extends MySQLCurrentRow {

        private final boolean[] firstBits;

        private int columnIndex = 0;


        private TextCurrentRow(MySQLRowMeta rowMeta) {
            super(rowMeta);
            this.firstBits = new boolean[rowMeta.columnMetaArray.length];
            resetFirstBits(this.firstBits);
        }


        @Override
        void doRest() {
            if (this.columnIndex != this.columnArray.length) {
                throw new IllegalStateException();
            }
            this.columnIndex = 0;
            resetFirstBits(this.firstBits);
        }

        /**
         * don't use {@link Arrays#fill(boolean[], boolean)}
         */
        static void resetFirstBits(final boolean[] firstBits) {
            final int length = firstBits.length;
            for (int i = 0; i < length; i++) {
                firstBits[i] = true;
            }

        }


    }//TextCurrentRow


}
