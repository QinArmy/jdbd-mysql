package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.BindValue;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLNumberUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.jdbd.vendor.statement.ParamValue;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


/**
 * @see ComPreparedTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
 */
final class PrepareExecuteCommandWriter implements StatementCommandWriter {

    private static final Logger LOG = LoggerFactory.getLogger(PrepareExecuteCommandWriter.class);

    private final StatementTask statementTask;

    private final int statementId;

    private final MySQLColumnMeta[] paramMetaArray;

    private final ClientProtocolAdjutant adjutant;

    private final boolean fetchResultSet;


    PrepareExecuteCommandWriter(final StatementTask statementTask) {
        this.statementTask = statementTask;
        this.statementId = statementTask.obtainStatementId();
        this.paramMetaArray = statementTask.obtainParameterMetas();
        this.adjutant = statementTask.obtainAdjutant();

        this.fetchResultSet = statementTask.isFetchResult();
    }


    @Override
    public Publisher<ByteBuf> writeCommand(final int stmtIndex, final List<? extends ParamValue> parameterGroup)
            throws SQLException {
        final MySQLColumnMeta[] paramMetaArray = this.paramMetaArray;
        BindUtils.assertParamCountMatch(stmtIndex, paramMetaArray.length, parameterGroup.size());

        final List<ParamValue> tempNonStreamParamGroup = new ArrayList<>(paramMetaArray.length);
        for (int i = 0; i < paramMetaArray.length; i++) {
            ParamValue paramValue = parameterGroup.get(i);
            if (paramValue.getParamIndex() != i) {
                // hear invoker has bug
                throw MySQLExceptions.createBindValueParamIndexNotMatchError(stmtIndex, paramValue, i);

            }
            if (!paramValue.isLongData()) {
                tempNonStreamParamGroup.add(paramValue);
            }
        }

        final List<ParamValue> nonStreamParamGroup = MySQLCollections.unmodifiableList(tempNonStreamParamGroup);

        final Publisher<ByteBuf> publisher;
        if (paramMetaArray.length == 0) {
            // this 'if' block handle no bind parameter.
            ByteBuf packet = createExecutePacketBuffer(10);
            PacketUtils.writePacketHeader(packet, this.statementTask.addAndGetSequenceId());
            publisher = Mono.just(packet);
        } else {
            final Publisher<ByteBuf> nonStreamPublisher;
            nonStreamPublisher = createExecutionPacketPublisher(stmtIndex, nonStreamParamGroup);
            if (nonStreamParamGroup.size() == paramMetaArray.length) {
                // this 'if' block handle no long parameter.
                publisher = nonStreamPublisher;
            } else {
                publisher = new PrepareLongParameterWriter(statementTask)
                        .write(stmtIndex, parameterGroup)
                        .concatWith(nonStreamPublisher);
            }
        }
        return publisher;
    }

    /*################################## blow private method ##################################*/


    /**
     * @see #writeCommand(int, List)
     */
    private Flux<ByteBuf> createExecutionPacketPublisher(final int stmtIndex, final List<ParamValue> parameterGroup) {
        Flux<ByteBuf> flux;
        try {
            flux = doCreateExecutionPackets(stmtIndex, parameterGroup);
        } catch (Throwable e) {
            flux = Flux.error(MySQLExceptions.wrap(e));
        }
        return flux;
    }

    /**
     * @return {@link Flux} that is created by {@link Flux#fromIterable(Iterable)} method.
     * @see #createExecutionPacketPublisher(int, List)
     */
    private Flux<ByteBuf> doCreateExecutionPackets(final int stmtIndex, final List<ParamValue> parameterGroup)
            throws JdbdException {
        final MySQLColumnMeta[] parameterMetaArray = this.paramMetaArray;
        final byte[] nullBitsMap = new byte[(parameterMetaArray.length + 7) / 8];

        final int parameterCount = parameterGroup.size();
        //1. make nullBitsMap and parameterValueLength
        long parameterValueLength = 0L;
        int paramIndex = 0;
        try {
            ParamValue paramValue;
            for (; paramIndex < parameterCount; paramIndex++) {
                paramValue = parameterGroup.get(paramIndex);
                if (paramValue.getValue() == null) {
                    nullBitsMap[paramIndex / 8] |= (1 << (paramIndex & 7));
                } else if (!paramValue.isLongData()) {
                    parameterValueLength += obtainParameterValueLength(stmtIndex, parameterMetaArray[paramIndex]
                            , paramValue);
                }
            }
        } catch (Throwable e) {
            throw MySQLExceptions.wrap(e, "Bind param[index(based zero):%s] type error.", paramIndex);
        }
        final int maxAllowedPayload = this.adjutant.obtainHostInfo().maxAllowedPayload();
        final long prefixLength = 10L + nullBitsMap.length + 1L + ((long) parameterCount << 1);
        final long payloadLength = prefixLength + parameterValueLength;
        if (payloadLength > maxAllowedPayload) {
            throw MySQLExceptions.createNetPacketTooLargeException(maxAllowedPayload);
        }

        ByteBuf packet;
        packet = createExecutePacketBuffer((int) payloadLength);

        packet.writeBytes(nullBitsMap); //fill null_bitmap
        packet.writeByte(1); //fill new_params_bind_flag
        //fill  parameter_types
        for (int i = 0; i < parameterMetaArray.length; i++) {
            PacketUtils.writeInt2(packet, parameterMetaArray[i].mysqlType.parameterType);
        }

        //fill parameter_values
        LinkedList<ByteBuf> packetList = new LinkedList<>();
        Flux<ByteBuf> flux;
        try {
            int resetBytes = ((int) payloadLength) - (int) prefixLength;
            ParamValue paramValue;
            for (int i = 0; i < parameterCount; i++) {
                paramValue = parameterGroup.get(i);
                if (paramValue.isLongData() || paramValue.getValue() == null) {
                    continue;
                }
                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    ByteBuf temp = packet.readRetainedSlice(PacketUtils.MAX_PACKET);
                    PacketUtils.writePacketHeader(temp, this.statementTask.addAndGetSequenceId());
                    packetList.add(temp);
                    resetBytes -= PacketUtils.MAX_PAYLOAD;

                    temp = this.adjutant.createPacketBuffer(Math.min(PacketUtils.MAX_PAYLOAD, resetBytes));
                    temp.writeBytes(packet);
                    packet = temp;
                }
                // bind parameter bto packet buffer
                bindParameter(packet, stmtIndex, parameterMetaArray[i], paramValue);
            }
            PacketUtils.writePacketHeader(packet, this.statementTask.addAndGetSequenceId());
            packetList.add(packet);

            flux = Flux.fromIterable(packetList);
        } catch (Throwable e) {
            BindUtils.releaseOnError(packetList, packet);
            flux = Flux.error(MySQLExceptions.wrap(e, "Bind parameter[index(based zero):%s] write error.", paramIndex));
        }
        return flux;
    }

    /**
     * @see #doCreateExecutionPackets(int, List)
     */
    private ByteBuf createExecutePacketBuffer(int initialPayloadCapacity) {

        ByteBuf packet = this.adjutant.createPacketBuffer(Math.min(initialPayloadCapacity, PacketUtils.MAX_PAYLOAD));

        packet.writeByte(PacketUtils.COM_STMT_EXECUTE); // 1.status
        PacketUtils.writeInt4(packet, this.statementId);// 2. statement_id
        //3.cursor Flags, reactive api not support cursor
        if (this.fetchResultSet) {
            packet.writeByte(ProtocolConstants.CURSOR_TYPE_READ_ONLY);
        } else {
            packet.writeByte(ProtocolConstants.CURSOR_TYPE_NO_CURSOR);
        }
        PacketUtils.writeInt4(packet, 1);//4. iteration_count,Number of times to execute the statement. Currently always 1.

        return packet;
    }

    /**
     * @return parameter value byte length ,if return {@link Integer#MIN_VALUE} ,then parameter error,should end task.
     * @throws IllegalArgumentException when {@link BindValue#getValue()} is null.
     * @see #createExecutePacketBuffer(int)
     */
    private long obtainParameterValueLength(final int stmtIndex, MySQLColumnMeta parameterMeta, ParamValue paramValue)
            throws SQLException {
        final Object value = paramValue.getRequiredValue();
        final long length;
        switch (parameterMeta.mysqlType) {
            case INT:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                length = 4L;
                break;
            case DATE:
                length = 5L;
                break;
            case BIGINT:
            case INT_UNSIGNED:
            case BIGINT_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case BIT:
                length = 8L;
                break;
            case BOOLEAN:
            case TINYINT:
            case TINYINT_UNSIGNED:
                length = 1L;
                break;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case YEAR:
                length = 2L;
                break;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                length = parameterMeta.length;
                break;
            case VARCHAR:
            case CHAR:
            case SET:
            case JSON:
            case ENUM:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB: {
                long lenEncBytes;
                if (value instanceof String) {
                    lenEncBytes = (long) ((String) value).length() * this.adjutant.obtainMaxBytesPerCharClient();
                } else if (value instanceof byte[]) {
                    lenEncBytes = ((byte[]) value).length;
                } else {
                    throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, parameterMeta.mysqlType
                            , paramValue);
                }
                length = PacketUtils.obtainIntLenEncLength(lenEncBytes) + lenEncBytes;
            }
            break;
            case DATETIME:
            case TIMESTAMP:
                length = (parameterMeta.decimals > 0 && parameterMeta.decimals < 7) ? 11 : 7;
                break;
            case TIME:
                length = (parameterMeta.decimals > 0 && parameterMeta.decimals < 7) ? 12 : 8;
                break;
            case GEOMETRY: {
                if (value instanceof byte[]) {
                    byte[] bytes = (byte[]) value;
                    length = bytes.length;
                } else {
                    length = 0;
                }
            }
            break;
            case NULL:
            case UNKNOWN:
                length = 0;
                break;
            default:
                throw MySQLExceptions.createUnknownEnumException(parameterMeta.mysqlType);
        }

        return length;
    }

    /**
     * @see #createExecutePacketBuffer(int)
     */
    private void bindParameter(ByteBuf buffer, int stmtIndex, MySQLColumnMeta meta, ParamValue paramValue)
            throws SQLException {

        switch (meta.mysqlType) {
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT:
            case INT_UNSIGNED:
                bindToInt4(buffer, stmtIndex, meta, paramValue);
                break;
            case BIGINT:
            case BIGINT_UNSIGNED:
                bindToInt8(buffer, stmtIndex, meta, paramValue);
                break;
            case FLOAT:
            case FLOAT_UNSIGNED:
                bindToFloat(buffer, stmtIndex, meta, paramValue);
                break;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                bindToDouble(buffer, stmtIndex, meta, paramValue);
                break;
            case BIT:
                bindToBit(buffer, stmtIndex, meta, paramValue);
                break;
            case BOOLEAN:
            case TINYINT:
            case TINYINT_UNSIGNED:
                bindToInt1(buffer, stmtIndex, meta, paramValue);
                break;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case YEAR:
                bindInt2(buffer, stmtIndex, meta, paramValue);
                break;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                bindToDecimal(buffer, stmtIndex, meta, paramValue);
                break;
            case ENUM:
            case VARCHAR:
            case CHAR:
            case JSON:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                // below binary
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:
            case GEOMETRY:
                bindToStringType(buffer, stmtIndex, meta.mysqlType, paramValue);
                break;
            case SET:
                bindToSetType(buffer, stmtIndex, meta, paramValue);
                break;
            case TIME:
                bindToTime(buffer, stmtIndex, meta, paramValue);
                break;
            case DATE:
                bindToDate(buffer, stmtIndex, meta, paramValue);
                break;
            case DATETIME:
            case TIMESTAMP:
                bindToDatetime(buffer, stmtIndex, meta, paramValue);
                break;
            case NULL:
            case UNKNOWN:
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, meta.mysqlType, paramValue);
            default:
                throw MySQLExceptions.createUnknownEnumException(meta.mysqlType);
        }
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToInt1(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final int int1;
        final int unsignedMaxByte = Byte.toUnsignedInt((byte) -1);
        if (nonNullValue instanceof Byte) {
            byte num = (Byte) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, parameterMeta.mysqlType, bindValue
                        , 0, unsignedMaxByte);
            }
            int1 = num;
        } else if (nonNullValue instanceof Boolean) {
            int1 = (Boolean) nonNullValue ? 1 : 0;
        } else if (nonNullValue instanceof Integer
                || nonNullValue instanceof Long
                || nonNullValue instanceof Short) {
            int1 = longTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType, ((Number) nonNullValue).longValue());
        } else if (nonNullValue instanceof String) {
            Number num;
            try {
                num = longTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType, Integer.parseInt((String) nonNullValue));
            } catch (NumberFormatException e) {
                try {
                    num = bigIntegerTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType
                            , new BigInteger((String) nonNullValue));
                } catch (NumberFormatException ne) {
                    throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
                }
            }
            int1 = num.intValue();
        } else if (nonNullValue instanceof BigInteger) {
            int1 = bigIntegerTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw MySQLExceptions.createNotSupportScaleException(stmtIndex, parameterMeta.mysqlType, bindValue);
            } else {
                int1 = bigIntegerTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType, num.toBigInteger());
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
        }

        PacketUtils.writeInt1(buffer, int1);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindInt2(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();

        final int unsignedMaxShort = Short.toUnsignedInt((short) -1);
        final int int2;
        if (nonNullValue instanceof Year) {
            int2 = ((Year) nonNullValue).getValue();
        } else if (nonNullValue instanceof Short) {
            short num = (Short) nonNullValue;
            if (parameterMeta.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, parameterMeta.mysqlType, bindValue
                        , 0, unsignedMaxShort);
            }
            int2 = num;
        } else if (nonNullValue instanceof Integer
                || nonNullValue instanceof Byte
                || nonNullValue instanceof Long) {
            int2 = longTotInt2(bindValue, stmtIndex, parameterMeta.mysqlType, ((Number) nonNullValue).longValue());
        } else if (nonNullValue instanceof String) {
            Number num;
            try {
                num = longTotInt2(bindValue, stmtIndex, parameterMeta.mysqlType
                        , Integer.parseInt((String) nonNullValue));
            } catch (NumberFormatException e) {
                try {
                    num = bigIntegerTotInt1(bindValue, stmtIndex, parameterMeta.mysqlType
                            , new BigInteger((String) nonNullValue));
                } catch (NumberFormatException ne) {
                    throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
                }
            }
            int2 = num.intValue();
        } else if (nonNullValue instanceof BigInteger) {
            int2 = bigIntegerTotInt2(bindValue, stmtIndex, parameterMeta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw MySQLExceptions.createNotSupportScaleException(stmtIndex, parameterMeta.mysqlType, bindValue);
            } else {
                int2 = bigIntegerTotInt2(bindValue, stmtIndex, parameterMeta.mysqlType, num.toBigInteger());
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
        }
        PacketUtils.writeInt2(buffer, int2);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value">Binary Protocol Value</a>
     */
    private void bindToDecimal(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue paramValue) {
        final Object nonNullValue = paramValue.getRequiredValue();
        final String decimal;
        if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            decimal = num.toPlainString();
        } else if (nonNullValue instanceof BigInteger) {
            BigInteger num = (BigInteger) nonNullValue;
            decimal = num.toString();
        } else if (nonNullValue instanceof String) {
            decimal = (String) nonNullValue;
        } else if (nonNullValue instanceof byte[]) {
            PacketUtils.writeStringLenEnc(buffer, (byte[]) nonNullValue);
            return;
        } else if (nonNullValue instanceof Integer
                || nonNullValue instanceof Long
                || nonNullValue instanceof Short
                || nonNullValue instanceof Byte) {
            decimal = BigInteger.valueOf(((Number) nonNullValue).longValue()).toString();
        } else if (nonNullValue instanceof Double || nonNullValue instanceof Float) {
            decimal = nonNullValue.toString();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, paramValue);
        }
        PacketUtils.writeStringLenEnc(buffer, decimal.getBytes(this.adjutant.obtainCharsetClient()));
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value">Binary Protocol Value</a>
     */
    private void bindToInt4(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue paramValue) {
        final Object nonNullValue = paramValue.getRequiredValue();
        final long unsignedMaxInt = Integer.toUnsignedLong(-1);
        final int int4;
        if (nonNullValue instanceof Integer) {
            int num = (Integer) nonNullValue;
            if (meta.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                        , 0, unsignedMaxInt);
            }
            int4 = num;
        } else if (nonNullValue instanceof Long) {
            int4 = longToInt4(stmtIndex, paramValue, meta.mysqlType, (Long) nonNullValue);
        } else if (nonNullValue instanceof String) {
            int num;
            try {
                num = longToInt4(stmtIndex, paramValue, meta.mysqlType, Long.parseLong((String) nonNullValue));
            } catch (NumberFormatException e) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                        , 0, unsignedMaxInt);

            }
            int4 = num;
        } else if (nonNullValue instanceof BigInteger) {
            int4 = bigIntegerToIn4(stmtIndex, paramValue, meta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof Short) {
            short num = ((Short) nonNullValue);
            if (meta.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                        , 0, unsignedMaxInt);
            }
            int4 = num;
        } else if (nonNullValue instanceof Byte) {
            byte num = ((Byte) nonNullValue);
            if (meta.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                        , 0, unsignedMaxInt);
            }
            int4 = num;
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw MySQLExceptions.createNotSupportScaleException(stmtIndex, meta.mysqlType, paramValue);
            } else if (meta.isUnsigned()) {
                if (num.compareTo(BigDecimal.ZERO) < 0
                        || num.compareTo(BigDecimal.valueOf(unsignedMaxInt)) > 0) {
                    throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                            , 0, unsignedMaxInt);
                }
            } else if (num.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0
                    || num.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, paramValue
                        , 0, unsignedMaxInt);
            }
            int4 = num.intValue();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, paramValue);
        }

        PacketUtils.writeInt4(buffer, int4);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToFloat(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final float floatValue;
        if (nonNullValue instanceof Float) {
            floatValue = (Float) nonNullValue;
        } else if (nonNullValue instanceof String) {
            try {
                floatValue = Float.parseFloat((String) nonNullValue);
            } catch (NumberFormatException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
            }
        } else if (nonNullValue instanceof Short) {
            floatValue = ((Short) nonNullValue).floatValue();
        } else if (nonNullValue instanceof Byte) {
            floatValue = ((Byte) nonNullValue).floatValue();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
        }

        PacketUtils.writeInt4(buffer, Float.floatToIntBits(floatValue));
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToInt8(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final long int8;
        if (nonNullValue instanceof Long) {
            long num = (Long) nonNullValue;
            if (meta.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, bindValue
                        , 0, MySQLNumberUtils.MAX_UNSIGNED_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof BigInteger) {
            int8 = bigIntegerToInt8(bindValue, stmtIndex, meta.mysqlType, (BigInteger) nonNullValue);
        } else if (nonNullValue instanceof Integer) {
            int num = (Integer) nonNullValue;
            if (meta.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, bindValue
                        , 0, MySQLNumberUtils.MAX_UNSIGNED_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof Short) {
            int num = (Short) nonNullValue;
            if (meta.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, bindValue
                        , 0, MySQLNumberUtils.MAX_UNSIGNED_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof Byte) {
            int num = (Byte) nonNullValue;
            if (meta.isUnsigned() && num < 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, meta.mysqlType, bindValue
                        , 0, MySQLNumberUtils.MAX_UNSIGNED_LONG);
            }
            int8 = num;
        } else if (nonNullValue instanceof String) {
            try {
                BigInteger big = new BigInteger((String) nonNullValue);
                int8 = bigIntegerToInt8(bindValue, stmtIndex, meta.mysqlType, big);
            } catch (NumberFormatException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
            }
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                throw MySQLExceptions.createNotSupportScaleException(stmtIndex, meta.mysqlType, bindValue);
            } else {
                int8 = bigIntegerToInt8(bindValue, stmtIndex, meta.mysqlType, num.toBigInteger());
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
        }

        PacketUtils.writeInt8(buffer, int8);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToDouble(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final double value;
        if (nonNullValue instanceof Double) {
            value = (Double) nonNullValue;
        } else if (nonNullValue instanceof Float) {
            value = (Float) nonNullValue;
        } else if (nonNullValue instanceof String) {
            try {
                value = Double.parseDouble((String) nonNullValue);
            } catch (NumberFormatException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
            }
        } else if (nonNullValue instanceof Integer) {
            value = ((Integer) nonNullValue).doubleValue();
        } else if (nonNullValue instanceof Short) {
            value = ((Short) nonNullValue).doubleValue();
        } else if (nonNullValue instanceof Byte) {
            value = ((Byte) nonNullValue).doubleValue();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
        }

        PacketUtils.writeInt8(buffer, Double.doubleToLongBits(value));
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToTime(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();

        final int microPrecision = parameterMeta.obtainDateTimeTypePrecision();
        final int length = microPrecision > 0 ? 12 : 8;

        if (nonNullValue instanceof Duration) {
            Duration duration = (Duration) nonNullValue;
            buffer.writeByte(length); //1. length

            buffer.writeByte(duration.isNegative() ? 1 : 0); //2. is_negative
            duration = duration.abs();
            if (!MySQLTimeUtils.canConvertToTimeType(duration)) {
                throw MySQLExceptions.createDurationRangeException(stmtIndex, parameterMeta.mysqlType, bindValue);
            }
            long temp;
            temp = duration.toDays();
            PacketUtils.writeInt4(buffer, (int) temp); //3. days
            duration = duration.minusDays(temp);

            temp = duration.toHours();
            buffer.writeByte((int) temp); //4. hour
            duration = duration.minusHours(temp);

            temp = duration.toMinutes();
            buffer.writeByte((int) temp); //5. minute
            duration = duration.minusMinutes(temp);

            temp = duration.getSeconds();
            buffer.writeByte((int) temp); //6. second
            duration = duration.minusSeconds(temp);
            if (length == 12) {
                //7, micro seconds
                PacketUtils.writeInt4(buffer, truncateMicroSeconds((int) duration.toMillis(), microPrecision));
            }
            return;
        }

        final LocalTime time;
        if (nonNullValue instanceof LocalTime) {
            time = OffsetTime.of((LocalTime) nonNullValue, this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } else if (nonNullValue instanceof OffsetTime) {
            time = ((OffsetTime) nonNullValue).withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } else if (nonNullValue instanceof String) {
            String timeText = (String) nonNullValue;
            try {
                time = OffsetTime.of(LocalTime.parse(timeText, MySQLTimeUtils.MYSQL_TIME_FORMATTER)
                        , this.adjutant.obtainZoneOffsetClient())
                        .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                        .toLocalTime();
            } catch (DateTimeParseException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue, e);
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
        }
        if (time != null) {
            buffer.writeByte(length); //1. length
            buffer.writeByte(0); //2. is_negative
            buffer.writeZero(4); //3. days

            buffer.writeByte(time.getHour()); //4. hour
            buffer.writeByte(time.getMinute()); //5. minute
            buffer.writeByte(time.getSecond()); ///6. second

            if (length == 12) {
                //7, micro seconds
                PacketUtils.writeInt4(buffer
                        , truncateMicroSeconds(time.get(ChronoField.MICRO_OF_SECOND), microPrecision));
            }
        }

    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToDate(final ByteBuf buffer, int stmtIndex, MySQLColumnMeta columnMeta, ParamValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();

        final LocalDate date;
        if (nonNullValue instanceof LocalDate) {
            date = (LocalDate) nonNullValue;

        } else if (nonNullValue instanceof String) {
            try {
                date = LocalDate.parse((String) nonNullValue);
            } catch (DateTimeParseException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, columnMeta.mysqlType, bindValue, e);
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, columnMeta.mysqlType, bindValue);
        }
        buffer.writeByte(4); // length
        PacketUtils.writeInt2(buffer, date.getYear()); // year
        buffer.writeByte(date.getMonthValue()); // month
        buffer.writeByte(date.getDayOfMonth()); // day
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToDatetime(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();

        final LocalDateTime dateTime;
        if (nonNullValue instanceof LocalDateTime) {
            dateTime = OffsetDateTime.of((LocalDateTime) nonNullValue, this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNullValue instanceof ZonedDateTime) {
            dateTime = ((ZonedDateTime) nonNullValue)
                    .withZoneSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNullValue instanceof OffsetDateTime) {
            dateTime = ((OffsetDateTime) nonNullValue)
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNullValue instanceof String) {
            try {
                LocalDateTime localDateTime = LocalDateTime.parse((String) nonNullValue
                        , MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
                dateTime = OffsetDateTime.of(localDateTime, this.adjutant.obtainZoneOffsetClient())
                        .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                        .toLocalDateTime();
            } catch (DateTimeParseException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue, e);
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.mysqlType, bindValue);
        }

        final int microPrecision = parameterMeta.obtainDateTimeTypePrecision();
        final int length = microPrecision > 0 ? 11 : 7;
        buffer.writeByte(length); // length

        PacketUtils.writeInt2(buffer, dateTime.getYear()); // year
        buffer.writeByte(dateTime.getMonthValue()); // month
        buffer.writeByte(dateTime.getDayOfMonth()); // day

        buffer.writeByte(dateTime.getHour()); // hour
        buffer.writeByte(dateTime.getMinute()); // minute
        buffer.writeByte(dateTime.getSecond()); // second

        if (length == 11) {
            // micro second
            PacketUtils.writeInt4(buffer
                    , truncateMicroSeconds(dateTime.get(ChronoField.MICRO_OF_SECOND), microPrecision));
        }

    }


    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToBit(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue)
            throws SQLException {
        final String bits = BindUtils.bindToBits(-1, parameterMeta.mysqlType, bindValue);
        if (bits.length() < 4 || bits.length() > (parameterMeta.length + 3)) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, parameterMeta.mysqlType, bindValue
                    , 1, parameterMeta.length);
        }
        PacketUtils.writeStringLenEnc(buffer, bits.getBytes(this.adjutant.obtainCharsetClient()));
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private void bindToStringType(final ByteBuf buffer, final int stmtIndex, final MySQLType mySQLType
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final Charset charset = this.adjutant.obtainCharsetClient();
        if (nonNullValue instanceof CharSequence || nonNullValue instanceof Character) {
            PacketUtils.writeStringLenEnc(buffer, nonNullValue.toString().getBytes(charset));
        } else if (nonNullValue instanceof byte[]) {
            PacketUtils.writeStringLenEnc(buffer, (byte[]) nonNullValue);
        } else if (nonNullValue instanceof Enum) {
            PacketUtils.writeStringLenEnc(buffer, ((Enum<?>) nonNullValue).name().getBytes(charset));
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, mySQLType, bindValue);
        }

    }

    private void bindToSetType(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        final String text;
        if (nonNullValue instanceof String) {
            text = (String) nonNullValue;
        } else if (nonNullValue instanceof Set) {
            Set<?> set = (Set<?>) nonNullValue;
            StringBuilder builder = new StringBuilder(set.size() * 6);
            int index = 0;
            for (Object o : set) {
                if (index > 0) {
                    builder.append(",");
                }
                if (o instanceof String) {
                    builder.append((String) o);
                } else if (o instanceof Enum) {
                    builder.append(((Enum<?>) o).name());
                } else {
                    throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
                }
                index++;
            }
            text = builder.toString();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.mysqlType, bindValue);
        }
        PacketUtils.writeStringLenEnc(buffer, text.getBytes(this.adjutant.obtainCharsetClient()));
    }


    /*################################## blow private static method ##################################*/

    /**
     * @see #bindToTime(ByteBuf, int, MySQLColumnMeta, ParamValue)
     * @see #bindToDatetime(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private static int truncateMicroSeconds(final int microSeconds, final int precision) {
        final int newMicroSeconds;
        switch (precision) {
            case 0:
                newMicroSeconds = 0;
                break;
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6: {
                int micro = microSeconds % 100_0000;
                int unit = 1;
                final int num = 6 - precision;
                for (int i = 0; i < num; i++) {
                    unit *= 10;
                }
                if (unit > 0) {
                    micro -= (micro % unit);
                }
                newMicroSeconds = micro;
            }
            break;
            default:
                throw new IllegalArgumentException(String.format("precision[%s] not in [0,6]", precision));
        }
        return newMicroSeconds;
    }


    /*################################## blow private static convert method ##################################*/

    /**
     * @see #bindToInt4(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private static int longToInt4(int stmtIndex, ParamValue bindValue, MySQLType mySQLType, final long num) {
        if (mySQLType.isUnsigned()) {
            final long unsignedMaxInt = Integer.toUnsignedLong(-1);
            if (num < 0 || num > unsignedMaxInt) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                        , 0, unsignedMaxInt);
            }
        } else if (num < Integer.MIN_VALUE || num > Integer.MAX_VALUE) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return (int) num;
    }

    /**
     * @see #bindToInt4(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private static int bigIntegerToIn4(int stmtIndex, ParamValue paramValue, MySQLType mySQLType
            , final BigInteger num) {
        if (mySQLType.isUnsigned()) {
            BigInteger unsignedMaxInt = BigInteger.valueOf(Integer.toUnsignedLong(-1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxInt) > 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, paramValue
                        , 0, unsignedMaxInt);
            }
        } else if (num.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, paramValue
                    , Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return num.intValue();
    }

    /**
     * @see #bindToInt8(ByteBuf, int, MySQLColumnMeta, ParamValue)
     */
    private static long bigIntegerToInt8(ParamValue bindValue, int stmtIndex, MySQLType mySQLType
            , final BigInteger num) {
        if (mySQLType.isUnsigned()) {
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(MySQLNumberUtils.MAX_UNSIGNED_LONG) > 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                        , 0, MySQLNumberUtils.MAX_UNSIGNED_LONG);
            }
        } else if (num.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Long.MIN_VALUE, Long.MAX_VALUE);
        }
        return num.longValue();
    }

    private static int longTotInt1(ParamValue bindValue, int stmtIndex, MySQLType mySQLType, final long num) {

        if (mySQLType.isUnsigned()) {
            int unsignedMaxByte = Byte.toUnsignedInt((byte) -1);
            if (num < 0 || num > unsignedMaxByte) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue, 0, unsignedMaxByte);
            }
        } else if (num < Byte.MIN_VALUE || num > Byte.MAX_VALUE) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        return (int) num;
    }

    private static int longTotInt2(ParamValue bindValue, int stmtIndex, MySQLType mySQLType, final long num) {

        if (mySQLType.isUnsigned()) {
            int unsignedMaxShort = Short.toUnsignedInt((short) -1);
            if (num < 0 || num > unsignedMaxShort) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                        , 0, unsignedMaxShort);
            }
        } else if (num < Short.MIN_VALUE || num > Short.MAX_VALUE) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Short.MIN_VALUE, Short.MAX_VALUE);
        }
        return (int) num;
    }

    private static int bigIntegerTotInt1(ParamValue bindValue, int stmtIndex, MySQLType mySQLType
            , final BigInteger num) {

        if (mySQLType.isUnsigned()) {
            BigInteger unsignedMaxByte = BigInteger.valueOf(Byte.toUnsignedInt((byte) -1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxByte) > 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                        , 0, unsignedMaxByte);
            }
        } else if (num.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) > 0) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Byte.MIN_VALUE, Byte.MAX_VALUE);
        }
        return num.intValue();
    }

    private static int bigIntegerTotInt2(ParamValue bindValue, int stmtIndex, MySQLType mySQLType
            , final BigInteger num) {

        if (mySQLType.isUnsigned()) {
            BigInteger unsignedMaxInt2 = BigInteger.valueOf(Short.toUnsignedInt((byte) -1));
            if (num.compareTo(BigInteger.ZERO) < 0 || num.compareTo(unsignedMaxInt2) > 0) {
                throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue, 0, unsignedMaxInt2);
            }
        } else if (num.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) < 0
                || num.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) > 0) {
            throw MySQLExceptions.createNumberRangErrorException(stmtIndex, mySQLType, bindValue
                    , Short.MIN_VALUE, Short.MAX_VALUE);
        }
        return num.intValue();
    }


}
