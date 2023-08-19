package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.mysql.util.*;
import io.jdbd.type.Point;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.util.JdbdExceptions;
import io.jdbd.vendor.util.JdbdSpatials;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.BitSet;
import java.util.List;
import java.util.function.IntSupplier;


/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 */
final class QueryCommandWriter {

    /**
     * @param stmt must be below stmt type:
     *             <ul>
     *                  <li>{@link StaticStmt}</li>
     *                  <li>{@link StaticMultiStmt}</li>
     *             </ul>
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     */
    static Publisher<ByteBuf> staticCommand(final Stmt stmt, final IntSupplier sequenceId,
                                            final TaskAdjutant adjutant) throws JdbdException {
        final Charset clientCharset = adjutant.charsetClient();
        final byte[] sqlBytes;
        if (stmt instanceof StaticStmt) {
            sqlBytes = ((StaticStmt) stmt).getSql().getBytes(clientCharset);
        } else if (stmt instanceof StaticMultiStmt) {
            sqlBytes = ((StaticMultiStmt) stmt).getMultiStmt().getBytes(clientCharset);
        } else {
            //no bug,never here
            throw new IllegalArgumentException("error stmt");
        }
        final ByteBuf packet;
        packet = Packets.createStmtPacket(adjutant.allocator(), stmt);
        packet.writeByte(Packets.COM_QUERY);

        try {
            if (Capabilities.supportQueryAttr(adjutant.capability())) {
                writeQueryAttribute(packet, stmt, adjutant);
            }
            packet.writeBytes(sqlBytes);
            return Packets.createPacketPublisher(packet, sequenceId, adjutant);
        } catch (Throwable e) {
            packet.release();
            throw MySQLExceptions.wrap(e);
        }


    }


    /**
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
     */
    static Publisher<ByteBuf> staticBatchCommand(final StaticBatchStmt stmt, IntSupplier sequenceId,
                                                 final TaskAdjutant adjutant) throws JdbdException {
        final List<String> sqlGroup = stmt.getSqlGroup();
        if (sqlGroup.isEmpty()) {
            throw MySQLExceptions.createQueryIsEmptyError();
        }
        final int sqlSize = sqlGroup.size();
        final Charset charsetClient = adjutant.charsetClient();
        if (!Charsets.isSupportCharsetClient(charsetClient)) {
            throw MySQLExceptions.notSupportClientCharset(charsetClient);
        }
        final ByteBuf packet;
        packet = Packets.createStmtPacket(adjutant.allocator(), stmt); // 1 representing SEMICOLON_BYTE
        packet.writeByte(Packets.COM_QUERY);

        try {
            if (Capabilities.supportQueryAttr(adjutant.capability())) {
                writeQueryAttribute(packet, stmt, adjutant);
            }
            final MySQLParser sqlParser = adjutant.sqlParser();
            String sql;
            for (int i = 0; i < sqlSize; i++) {
                if (i > 0) {
                    packet.writeByte(Constants.SEMICOLON_BYTE);
                }
                sql = sqlGroup.get(i);
                if (!sqlParser.isSingleStmt(sql)) {
                    throw MySQLExceptions.createMultiStatementError();
                }
                packet.writeBytes(sql.getBytes(charsetClient));
            }
            return Packets.createPacketPublisher(packet, sequenceId, adjutant);
        } catch (Throwable e) {
            packet.release();
            throw MySQLExceptions.wrap(e);
        }


    }


    /**
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     */
    static Publisher<ByteBuf> bindableCommand(ParamStmt bindStmt, IntSupplier sequenceId, TaskAdjutant adjutant)
            throws JdbdException {
        return new QueryCommandWriter(sequenceId, adjutant)
                .writeBindableCommand(bindStmt);
    }


    /**
     * @return a unmodifiable Iterable.
     */
    static Publisher<ByteBuf> bindableMultiCommand(final ParamMultiStmt stmt, IntSupplier sequenceId,
                                                   TaskAdjutant adjutant) throws JdbdException {
        return new QueryCommandWriter(sequenceId, adjutant)
                .writeMultiCommand(stmt);
    }


    /**
     * @return a unmodifiable list.
     */
    static Publisher<ByteBuf> bindableBatchCommand(ParamBatchStmt stmt, IntSupplier sequenceIdSupplier,
                                                   TaskAdjutant adjutant) throws JdbdException {

        return new QueryCommandWriter(sequenceIdSupplier, adjutant)
                .writeBindableBatchCommand(stmt);
    }


    private final IntSupplier sequenceId;

    private final TaskAdjutant adjutant;

    private final boolean hexEscape;

    private final Charset clientCharset;

    private final boolean supportZoneOffset;


    private QueryCommandWriter(final IntSupplier sequenceId, final TaskAdjutant adjutant) {
        this.sequenceId = sequenceId;
        this.adjutant = adjutant;
        this.hexEscape = Terminator.isNoBackslashEscapes(adjutant.serverStatus());
        this.clientCharset = adjutant.charsetClient();
        this.supportZoneOffset = adjutant.handshake10().serverVersion.isSupportZoneOffset();

    }


    /*################################## blow private method ##################################*/


    /**
     * @return a unmodifiable list.
     * @see #bindableMultiCommand(ParamMultiStmt, IntSupplier, TaskAdjutant)
     */
    private Publisher<ByteBuf> writeMultiCommand(final ParamMultiStmt multiStmt)
            throws JdbdException {
        final List<ParamStmt> stmtGroup = multiStmt.getStmtList();
        final int size = stmtGroup.size();
        final MySQLParser parser = this.adjutant.sqlParser();
        final ByteBuf packet;
        packet = Packets.createStmtPacket(this.adjutant.allocator(), multiStmt);
        packet.writeByte(Packets.COM_QUERY);
        try {
            if (Capabilities.supportQueryAttr(this.adjutant.capability())) {
                writeQueryAttribute(packet, multiStmt, this.adjutant);
            }
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    packet.writeByte(Constants.SEMICOLON_BYTE);
                }
                final ParamStmt stmt = stmtGroup.get(i);
                final List<String> staticSqlList = parser.parse(stmt.getSql()).sqlPartList();
                doWriteBindableCommand(i, staticSqlList, stmt.getParamGroup(), packet);
            }
            return Packets.createPacketPublisher(packet, this.sequenceId, this.adjutant);
        } catch (Throwable e) {
            packet.release();
            throw e;
        }
    }

    /**
     * @return a unmodifiable list.
     * @see #bindableCommand(ParamStmt, IntSupplier, TaskAdjutant)
     */
    private Publisher<ByteBuf> writeBindableCommand(final ParamStmt stmt) throws JdbdException {
        final TaskAdjutant adjutant = this.adjutant;

        final List<String> staticSqlList;
        staticSqlList = adjutant.parse(stmt.getSql()).sqlPartList();
        final ByteBuf packet;
        packet = Packets.createStmtPacket(adjutant.allocator(), stmt);
        packet.writeByte(Packets.COM_QUERY);
        try {
            if (Capabilities.supportQueryAttr(adjutant.capability())) {
                writeQueryAttribute(packet, stmt, adjutant);
            }
            doWriteBindableCommand(-1, staticSqlList, stmt.getParamGroup(), packet);
            return Packets.createPacketPublisher(packet, this.sequenceId, adjutant);
        } catch (Throwable e) {
            packet.release();
            throw JdbdExceptions.wrap(e);
        }

    }

    /**
     * @return a unmodifiable list.
     * @see #bindableBatchCommand(ParamBatchStmt, IntSupplier, TaskAdjutant)
     */
    private Publisher<ByteBuf> writeBindableBatchCommand(final ParamBatchStmt stmt)
            throws JdbdException {

        final List<String> staticSqlList = this.adjutant.sqlParser().parse(stmt.getSql()).getStaticSql();

        final List<List<ParamValue>> parameterGroupList = stmt.getGroupList();
        final int stmtCount = parameterGroupList.size();

        final ByteBuf packet;
        packet = Packets.createStmtPacket(this.adjutant.allocator(), stmt);
        packet.writeByte(Packets.COM_QUERY);
        try {
            if (Capabilities.supportQueryAttr(this.adjutant.capability())) {
                writeQueryAttribute(packet, stmt, this.adjutant);
            }
            for (int i = 0; i < stmtCount; i++) {
                if (i > 0) {
                    packet.writeByte(Constants.SEMICOLON_BYTE); // write ';' delimit multiple statement.
                }
                doWriteBindableCommand(i, staticSqlList, parameterGroupList.get(i), packet);
            }
            return Packets.createPacketPublisher(packet, this.sequenceId, this.adjutant);
        } catch (Throwable e) {
            packet.release();
            throw JdbdExceptions.wrap(e);
        }
    }


    /**
     * @see #writeMultiCommand(ParamMultiStmt)
     * @see #writeBindableCommand(ParamStmt)
     */
    private void doWriteBindableCommand(final int batchIndex, final List<String> staticSqlList,
                                        final List<ParamValue> parameterGroup, final ByteBuf packet)
            throws JdbdException {

        final int paramCount = staticSqlList.size() - 1;
        MySQLBinds.assertParamCountMatch(batchIndex, paramCount, parameterGroup.size());
        final Charset clientCharset = this.clientCharset;
        final byte[] nullBytes = Constants.NULL.getBytes(clientCharset);

        ParamValue paramValue;
        Object value;
        for (int i = 0; i < paramCount; i++) {
            paramValue = parameterGroup.get(i);
            if (paramValue.getIndex() != i) {
                // hear invoker has bug
                throw MySQLExceptions.bindValueParamIndexNotMatchError(batchIndex, paramValue, i);
            }
            packet.writeBytes(staticSqlList.get(i).getBytes(clientCharset));

            value = paramValue.getValue();
            if (value == null) {
                packet.writeBytes(nullBytes);
                continue;
            }
            if (value instanceof Publisher || value instanceof Path) {
                // Statement no bug,never here
                throw MySQLExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
            }
            writeParameter(batchIndex, paramValue, packet);
        }
        // write last static sql
        packet.writeBytes(staticSqlList.get(paramCount).getBytes(clientCharset));

    }


    /**
     * @see #doWriteBindableCommand(int, List, List, ByteBuf)
     */
    @SuppressWarnings("deprecation")
    private void writeParameter(final int batchIndex, final ParamValue paramValue, final ByteBuf packet)
            throws JdbdException {

        try {
            final MySQLType type = (MySQLType) paramValue.getType();
            switch (type) {
                case TINYINT: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, Byte.MIN_VALUE, Byte.MAX_VALUE);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case TINYINT_UNSIGNED: {
                    final int value;
                    value = MySQLBinds.bindToIntUnsigned(batchIndex, paramValue, 0xFF);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case SMALLINT: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, Short.MIN_VALUE, Short.MAX_VALUE);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case SMALLINT_UNSIGNED: {
                    final int value;
                    value = MySQLBinds.bindToIntUnsigned(batchIndex, paramValue, 0xFFFF);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case MEDIUMINT: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, 0x8000_00, 0xFFFF_FF);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case MEDIUMINT_UNSIGNED: {
                    final int value;
                    value = MySQLBinds.bindToIntUnsigned(batchIndex, paramValue, 0xFFFF_FF);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case INT: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case INT_UNSIGNED: {
                    final int value;
                    value = MySQLBinds.bindToIntUnsigned(batchIndex, paramValue, -1);
                    packet.writeBytes(Long.toString(value).getBytes(this.clientCharset));
                }
                break;
                case BIGINT: {
                    final long value;
                    value = MySQLBinds.bindToLong(batchIndex, paramValue, Long.MIN_VALUE, Long.MAX_VALUE);
                    packet.writeBytes(Long.toString(value).getBytes(this.clientCharset));
                }
                break;
                case BIGINT_UNSIGNED: {
                    final long value;
                    value = MySQLBinds.bindToLongUnsigned(batchIndex, paramValue, -1L);
                    packet.writeBytes(Long.toString(value).getBytes(this.clientCharset));
                }
                break;
                case DECIMAL: {
                    final BigDecimal value;
                    value = MySQLBinds.bindToDecimal(batchIndex, paramValue);
                    packet.writeBytes(value.toPlainString().getBytes(this.clientCharset));
                }
                break;
                case DECIMAL_UNSIGNED: {
                    final BigDecimal value;
                    value = MySQLBinds.bindToDecimal(batchIndex, paramValue);
                    if (value.compareTo(BigDecimal.ZERO) < 0) {
                        throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue, null);
                    }
                    packet.writeBytes(value.toPlainString().getBytes(this.clientCharset));
                }
                break;
                case FLOAT: {
                    final float value;
                    value = MySQLBinds.bindToFloat(batchIndex, paramValue);
                    packet.writeBytes(Float.toString(value).getBytes(this.clientCharset));
                }
                break;
                case FLOAT_UNSIGNED: {
                    final float value;
                    value = MySQLBinds.bindToFloat(batchIndex, paramValue);
                    if (value < 0.0f) {
                        throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue, null);
                    }
                    packet.writeBytes(Float.toString(value).getBytes(this.clientCharset));
                }
                break;
                case DOUBLE: {
                    final double value;
                    value = MySQLBinds.bindToDouble(batchIndex, paramValue);
                    packet.writeBytes(Double.toString(value).getBytes(this.clientCharset));
                }
                break;
                case DOUBLE_UNSIGNED: {
                    final double value;
                    value = MySQLBinds.bindToDouble(batchIndex, paramValue);
                    if (value < 0.0d) {
                        throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue, null);
                    }
                    packet.writeBytes(Double.toString(value).getBytes(this.clientCharset));
                }
                break;
                case YEAR:
                    writeYearValue(batchIndex, paramValue, packet);
                    break;
                case BOOLEAN: {
                    final boolean value;
                    value = MySQLBinds.bindToBoolean(batchIndex, paramValue);
                    final String v = value ? Constants.TRUE : Constants.FALSE;
                    packet.writeBytes(v.getBytes(this.clientCharset));
                }
                break;
                case BIT:
                    writeBitValue(batchIndex, paramValue, packet);
                    break;
                case CHAR:
                case VARCHAR:
                case ENUM:
                case TINYTEXT:
                case MEDIUMTEXT:
                case TEXT:
                case LONGTEXT: {
                    final byte[] byteArray;
                    byteArray = MySQLBinds.bindToString(batchIndex, paramValue).getBytes(this.clientCharset);
                    writeOneEscapesValue(packet, byteArray);
                }
                break;
                case JSON: {
                    final Object value;
                    value = MySQLBinds.bindToJson(batchIndex, paramValue);
                    if (value instanceof String) {
                        writeOneEscapesValue(packet, ((String) value).getBytes(this.clientCharset));
                    } else if (value instanceof BigDecimal) {
                        packet.writeBytes(((BigDecimal) value).toPlainString().getBytes(this.clientCharset));
                    } else if (value instanceof Number) {
                        packet.writeBytes(value.toString().getBytes(this.clientCharset));
                    } else {
                        // no bug,never here
                        throw new IllegalStateException("bind json error");
                    }
                }
                break;
                // below binary
                case BINARY:
                case VARBINARY:
                case TINYBLOB:
                case MEDIUMBLOB:
                case BLOB:
                case LONGBLOB: {
                    final Object value;
                    value = paramValue.getNonNullValue();
                    if (!(value instanceof byte[])) {
                        throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue, null);
                    }
                    writeHexEscape(packet, (byte[]) value);
                }
                break;
                case SET: {
                    final String value;
                    value = MySQLBinds.bindToSetType(batchIndex, paramValue);
                    writeOneEscapesValue(packet, value.getBytes(this.clientCharset));
                }
                break;
                case TIME:
                    writeTimeValue(batchIndex, paramValue, packet);
                    break;
                case DATE:
                    writeDateValue(batchIndex, paramValue, packet);
                    break;
                case DATETIME:
                case TIMESTAMP:
                    writeDateTimeValue(batchIndex, paramValue, packet);
                    break;
                case GEOMETRY: {
                    final Object nonNull = paramValue.getNonNullValue();
                    if (nonNull instanceof Point) {
                        writeHexEscape(packet, JdbdSpatials.writePointToWkb(false, (Point) nonNull));
                    } else if (nonNull instanceof byte[]) {
                        writeHexEscape(packet, (byte[]) nonNull);
                    } else if (nonNull instanceof String) {
                        writeOneEscapesValue(packet, ((String) nonNull).getBytes(this.clientCharset));
                    } else {
                        throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                    }
                }
                break;
                case UNKNOWN:
                case NULL:
                default:
                    throw MySQLExceptions.createUnsupportedParamTypeError(batchIndex, paramValue);
            }
        } catch (JdbdException e) {
            throw e;
        } catch (DateTimeException | NumberFormatException | ArithmeticException e) {
            throw MySQLExceptions.outOfTypeRange(batchIndex, paramValue, e);
        } catch (Throwable e) {
            throw MySQLExceptions.wrap(e);
        }

    }


    private void writeOneEscapesValue(final ByteBuf packet, final byte[] value) {
        if (this.hexEscape) {
            packet.writeByte('X');
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
            packet.writeBytes(MySQLBuffers.hexEscapes(true, value, value.length));
        } else {
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
            writeByteEscapes(packet, value, value.length);
        }
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
    }

    private void writeHexEscape(final ByteBuf packet, final byte[] value) {
        packet.writeByte('X');
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(MySQLBuffers.hexEscapes(true, value, value.length));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
    }


    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeYearValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {
        final Object nonNull = bindValue.getNonNull();
        final int value;
        if (nonNull instanceof Year) {
            value = ((Year) nonNull).getValue();
        } else if (nonNull instanceof Integer) {
            value = (Integer) nonNull;
        } else if (nonNull instanceof Short) {
            value = (Short) nonNull;
        } else {
            throw MySQLExceptions.nonSupportBindSqlTypeError(batchIndex, bindValue);
        }
        packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
    }

    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeBitValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {

        final String value;
        value = bindToBit(batchIndex, bindValue);

        packet.writeByte('B');
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
    }


    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeTimeValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {

        final Object nonNull = bindValue.getNonNullValue();
        final String value;
        if (nonNull instanceof Duration) {
            value = MySQLTimes.durationToTimeText((Duration) nonNull);
        } else {
            value = MySQLBinds.bindToLocalTime(batchIndex, bindValue)
                    .format(MySQLTimes.TIME_FORMATTER_6);
        }
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);

    }

    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeDateValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {

        final String value;
        value = MySQLBinds.bindToLocalDate(batchIndex, bindValue)
                .format(DateTimeFormatter.ISO_LOCAL_DATE);

        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
    }

    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html">Date and Time Literals</a>
     */
    private void writeDateTimeValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {
        final Object nonNull = bindValue.getNonNullValue();

        final String value;
        if (this.supportZoneOffset && (nonNull instanceof OffsetDateTime || nonNull instanceof ZonedDateTime)) {
            value = MySQLTimes.OFFSET_DATETIME_FORMATTER_6.format((TemporalAccessor) nonNull);
        } else {
            value = MySQLBinds.bindToLocalDateTime(batchIndex, bindValue).format(MySQLTimes.DATETIME_FORMATTER_6);
        }
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);

    }


    /**
     * @see #writeBitValue(int, ParamValue, ByteBuf)
     */
    private static String bindToBit(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getValue();
        final String value;

        if (nonNull instanceof Long) {
            value = Long.toBinaryString((Long) nonNull);
        } else if (nonNull instanceof Integer) {
            value = Integer.toBinaryString((Integer) nonNull);
        } else if (nonNull instanceof Short) {
            value = Integer.toBinaryString(((Short) nonNull) & 0xFFFF);
        } else if (nonNull instanceof Byte) {
            value = Integer.toBinaryString(((Byte) nonNull) & 0xFF);
        } else if (nonNull instanceof BitSet) {
            final BitSet v = (BitSet) nonNull;
            if (v.length() > 64) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
            value = MySQLStrings.bitSetToBitString(v, true);
        } else if (nonNull instanceof String) {
            final String v = (String) nonNull;
            if (v.length() > 64 || !MySQLStrings.isBinaryString(v)) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
            value = v;
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }


    private void writeByteEscapes(final ByteBuf packet, final byte[] bytes, final int length) {
        if (length < 0 || length > bytes.length) {
            // no bug,never here
            String m = String.format("length[%s] and bytes.length[%s] not match.", length, bytes.length);
            throw new IllegalArgumentException(m);
        }
        int lastWritten = 0;
        for (int i = 0; i < length; i++) {
            byte b = bytes[i];
            if (b == Constants.EMPTY_CHAR_BYTE) {
                if (i > lastWritten) {
                    packet.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                packet.writeByte(Constants.BACK_SLASH_BYTE);
                packet.writeByte('0');
                lastWritten = i + 1;
            } else if (b == '\032') {
                if (i > lastWritten) {
                    packet.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                packet.writeByte(Constants.BACK_SLASH_BYTE);
                packet.writeByte('Z');
                lastWritten = i + 1;
            } else if (b == Constants.BACK_SLASH_BYTE
                    || b == Constants.QUOTE_CHAR_BYTE
                    || b == Constants.DOUBLE_QUOTE_BYTE) {
                if (i > lastWritten) {
                    packet.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                packet.writeByte(Constants.BACK_SLASH_BYTE);
                lastWritten = i; // not i+1 as b wasn't written.
            }

        }

        if (lastWritten < length) {
            packet.writeBytes(bytes, lastWritten, length - lastWritten);
        }


    }


    /*################################## blow private static method ##################################*/


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
     */
    private static void writeQueryAttribute(final ByteBuf packet, final Stmt stmt, final TaskAdjutant adjutant) {

        final List<NamedValue> attrList;
        attrList = stmt.getStmtVarList();

        final int paramCount = attrList.size();
        Packets.writeIntLenEnc(packet, paramCount);// Number of parameters
        Packets.writeIntLenEnc(packet, 1);// Number of parameter sets. Currently, always 1

        if (paramCount == 0) {
            return;
        }

        final byte[] nullBitMap = new byte[(paramCount + 7) >> 3];
        final int nullBitMapWriterIndex = packet.writerIndex();
        packet.writeZero(nullBitMap.length); //placeholder of nullBitMap
        packet.writeByte(1); // new_params_bind_flag.Always 1. Malformed packet error if not 1

        final Charset clientCharset;
        clientCharset = adjutant.charsetClient();

        final boolean supportZoneOffset;
        supportZoneOffset = adjutant.handshake10().serverVersion.isSupportZoneOffset();

        // write param_type_and_flag and parameter name
        BinaryWriter.writeQueryAttrType(packet, attrList, nullBitMap, clientCharset, supportZoneOffset);

        // below write nullBitMap bytes
        Packets.writeBytesAtIndex(packet, nullBitMap, nullBitMapWriterIndex);

        // below write parameter_values for query attribute

        final ZoneOffset serverZone;
        serverZone = adjutant.serverZone();
        boolean useServerZone;
        NamedValue namedValue;
        for (int i = 0; i < paramCount; i++) {
            namedValue = attrList.get(i);
            if (namedValue.getValue() == null) {
                continue;
            }
            useServerZone = supportZoneOffset && namedValue.getType() == MySQLType.DATETIME;
            BinaryWriter.writeBinary(packet, -1, namedValue, -1, clientCharset, useServerZone ? serverZone : null);
        }

    }


}
