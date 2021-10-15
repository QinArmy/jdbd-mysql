package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.type.CodeEnum;
import io.jdbd.vendor.stmt.ParamValue;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


/**
 * @see ComPreparedStmtTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
 */
final class PrepareExecuteCommandWriter implements ExecuteCommandWriter {

    static ExecuteCommandWriter create(StmtTask stmtTask) {
        return null;
    }


    private final PrepareStmtTask statementTask;

    private final int statementId;

    private final MySQLColumnMeta[] paramMetaArray;

    private final ClientProtocolAdjutant adjutant;

    private final boolean fetchResultSet;


    PrepareExecuteCommandWriter(final PrepareStmtTask statementTask) {
        this.statementTask = statementTask;
        this.statementId = statementTask.obtainStatementId();
        this.paramMetaArray = statementTask.obtainParameterMetas();
        this.adjutant = statementTask.obtainAdjutant();

        this.fetchResultSet = statementTask.supportFetch();
    }


    @Override
    public Publisher<ByteBuf> writeCommand(final int stmtIndex, final List<? extends ParamValue> parameterGroup)
            throws SQLException {
        final MySQLColumnMeta[] paramMetaArray = this.paramMetaArray;
        MySQLBinds.assertParamCountMatch(stmtIndex, paramMetaArray.length, parameterGroup.size());

        int nonLongDataCount = 0;
        for (int i = 0; i < paramMetaArray.length; i++) {
            ParamValue paramValue = parameterGroup.get(i);
            if (paramValue.getIndex() != i) {
                // hear invoker has bug
                throw MySQLExceptions.createBindValueParamIndexNotMatchError(stmtIndex, paramValue, i);
            }
            if (!paramValue.isLongData()) {
                nonLongDataCount++;
            }
        }

        final Publisher<ByteBuf> publisher;
        if (paramMetaArray.length == 0) {
            // this 'if' block handle no bind parameter.
            ByteBuf packet = createExecutePacketBuffer(10);
            Packets.writePacketHeader(packet, this.statementTask.safelyAddAndGetSequenceId());
            publisher = Mono.just(packet);
        } else {
            if (nonLongDataCount == paramMetaArray.length) {
                // this 'if' block handle no long parameter.
                publisher = createExecutionPackets(stmtIndex, parameterGroup);
            } else {
                // start safe sequence id
                this.statementTask.startSafeSequenceId();
                publisher = new PrepareLongParameterWriter(this.statementTask)
                        .write(stmtIndex, parameterGroup)
                        .concatWith(defferCreateExecutionPackets(stmtIndex, parameterGroup))
                        // below end safe sequence id
                        .doOnError(error -> this.statementTask.endSafeSequenceId())
                        .doOnComplete(this.statementTask::endSafeSequenceId);
            }
        }
        return publisher;
    }

    @Override
    public Publisher<ByteBuf> writeCommand(int stmtIndex) throws SQLException {
        return null;
    }

    /*################################## blow private method ##################################*/

    private Flux<ByteBuf> defferCreateExecutionPackets(final int stmtIndex
            , final List<? extends ParamValue> parameterGroup) {
        return Flux.defer(() -> {
            Flux<ByteBuf> flux;
            try {
                flux = createExecutionPackets(stmtIndex, parameterGroup);
            } catch (Throwable e) {
                flux = Flux.error(MySQLExceptions.wrap(e));
            }
            return flux;
        });
    }


    /**
     * @return {@link Flux} that is created by {@link Flux#fromIterable(Iterable)} method.
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
     */
    private Flux<ByteBuf> createExecutionPackets(final int stmtIndex, final List<? extends ParamValue> parameterGroup)
            throws JdbdException, SQLException {


        final MySQLColumnMeta[] parameterMetaArray = this.paramMetaArray;
        MySQLBinds.assertParamCountMatch(stmtIndex, parameterMetaArray.length, parameterGroup.size());

        ByteBuf packet;
        packet = createExecutePacketBuffer(1024);

        //fill parameter_values
        LinkedList<ByteBuf> packetList = new LinkedList<>();
        Flux<ByteBuf> flux;
        try {
            final int nullBitsMapIndex = packet.writerIndex();
            final byte[] nullBitsMap = new byte[(parameterMetaArray.length + 7) >> 3];
            packet.writeZero(nullBitsMap.length); // placeholder for fill null_bitmap
            packet.writeByte(1); //fill new_params_bind_flag

            //1. make nullBitsMap and fill  parameter_types
            final List<MySQLType> bindTypeList = new ArrayList<>(parameterMetaArray.length);
            for (int i = 0; i < parameterMetaArray.length; i++) {
                ParamValue paramValue = parameterGroup.get(i);
                if (paramValue.get() == null) {
                    nullBitsMap[i >> 3] |= (1 << (i & 7));
                }
                MySQLType bindType = decideBindType(stmtIndex, parameterMetaArray[i], paramValue);
                bindTypeList.add(bindType);
                //fill  parameter_types
                Packets.writeInt2(packet, bindType.parameterType);
            }

            final int writeIndex = packet.writerIndex();
            packet.writerIndex(nullBitsMapIndex);

            packet.writeBytes(nullBitsMap); //fill null_bitmap

            packet.writerIndex(writeIndex); // reset writeIndex

            ParamValue paramValue;
            final int maxAllowedPayload = this.adjutant.obtainHostInfo().maxAllowedPayload();
            int wroteBytes = 0;

            for (int i = 0; i < parameterMetaArray.length; i++) {
                paramValue = parameterGroup.get(i);
                if (paramValue.isLongData() || paramValue.get() == null) {
                    continue;
                }
                while (packet.readableBytes() >= Packets.MAX_PACKET) {
                    ByteBuf temp = packet.readRetainedSlice(Packets.MAX_PACKET);
                    Packets.writePacketHeader(temp, this.statementTask.safelyAddAndGetSequenceId());
                    packetList.add(temp);
                    wroteBytes += Packets.MAX_PAYLOAD;

                    temp = this.adjutant.createPacketBuffer(Math.min(1024, packet.readableBytes()));
                    temp.writeBytes(packet);
                    packet.release();
                    packet = temp;

                    if (wroteBytes < 0 || wroteBytes > maxAllowedPayload) {
                        throw MySQLExceptions.createNetPacketTooLargeException(maxAllowedPayload);
                    }
                }

                // bind parameter bto packet buffer
                bindParameter(packet, stmtIndex, bindTypeList.get(i), parameterMetaArray[i], paramValue);
            }
            wroteBytes += (packet.readableBytes() - Packets.HEADER_SIZE);
            if (wroteBytes < 0 || wroteBytes > maxAllowedPayload) {
                throw MySQLExceptions.createNetPacketTooLargeException(maxAllowedPayload);
            }

            Packets.writePacketHeader(packet, this.statementTask.safelyAddAndGetSequenceId());
            packetList.add(packet);


            flux = Flux.fromIterable(packetList);
        } catch (Throwable e) {
            MySQLBinds.releaseOnError(packetList, packet);
            flux = Flux.error(MySQLExceptions.wrap(e));
        }
        return flux;
    }


    /**
     * @see #createExecutionPackets(int, List)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
     */
    private ByteBuf createExecutePacketBuffer(int initialPayloadCapacity) {

        ByteBuf packet = this.adjutant.createPacketBuffer(Math.min(initialPayloadCapacity, Packets.MAX_PAYLOAD));

        packet.writeByte(Packets.COM_STMT_EXECUTE); // 1.status
        Packets.writeInt4(packet, this.statementId);// 2. statement_id
        //3.cursor Flags, reactive api not support cursor
        if (this.fetchResultSet) {
            packet.writeByte(Constants.CURSOR_TYPE_READ_ONLY);
        } else {
            packet.writeByte(Constants.CURSOR_TYPE_NO_CURSOR);
        }
        Packets.writeInt4(packet, 1);//4. iteration_count,Number of times to execute the statement. Currently always 1.

        return packet;
    }

    private MySQLType decideBindType(int stmtIndex, MySQLColumnMeta meta, ParamValue paramValue) {
        final Object nonNull = paramValue.getNonNull();
        final MySQLType targetType = meta.sqlType;
        final MySQLType bindType;
        if (nonNull instanceof Number) {
            if (nonNull instanceof Long) {
                bindType = MySQLType.BIGINT;
            } else if (nonNull instanceof Integer) {
                bindType = MySQLType.INT;
            } else if (nonNull instanceof Short) {
                bindType = MySQLType.SMALLINT;
            } else if (nonNull instanceof Byte) {
                bindType = MySQLType.TINYINT;
            } else if (nonNull instanceof Double) {
                bindType = MySQLType.DOUBLE;
            } else if (nonNull instanceof Float) {
                bindType = MySQLType.FLOAT;
            } else if (nonNull instanceof BigDecimal || nonNull instanceof BigInteger) {
                bindType = MySQLType.DECIMAL;
            } else {
                throw MySQLExceptions.createWrongArgumentsException(stmtIndex, meta.sqlType, paramValue, null);
            }
        } else if (nonNull instanceof Boolean) {
            if (targetType == MySQLType.CHAR
                    || targetType == MySQLType.VARCHAR) {
                bindType = targetType;
            } else {
                bindType = MySQLType.TINYINT;
            }
        } else if (nonNull instanceof String) {
            if (targetType == MySQLType.DATETIME
                    || targetType == MySQLType.TIMESTAMP
                    || targetType == MySQLType.TIME
                    || targetType == MySQLType.JSON) {
                bindType = targetType;
            } else {
                bindType = MySQLType.VARCHAR;
            }
        } else if (nonNull instanceof Temporal) {
            if (nonNull instanceof LocalDateTime
                    || nonNull instanceof OffsetDateTime
                    || nonNull instanceof ZonedDateTime) {
                bindType = MySQLType.DATETIME;
            } else if (nonNull instanceof LocalDate
                    || nonNull instanceof YearMonth) {
                bindType = MySQLType.DATE;
            } else if (nonNull instanceof LocalTime
                    || nonNull instanceof OffsetTime) {
                bindType = MySQLType.TIME;
            } else if (nonNull instanceof Year) {
                bindType = MySQLType.SMALLINT;
            } else if (nonNull instanceof Instant) {
                bindType = MySQLType.BIGINT;
            } else {
                throw MySQLExceptions.createWrongArgumentsException(stmtIndex, meta.sqlType, paramValue, null);
            }
        } else if (nonNull instanceof byte[]) {
            bindType = MySQLType.VARBINARY;
        } else if (nonNull instanceof TemporalAccessor) {
            if (targetType == MySQLType.JSON) {
                bindType = targetType;
            } else if (nonNull instanceof MonthDay) {
                bindType = MySQLType.DATE;
            } else if (nonNull instanceof Month
                    || nonNull instanceof DayOfWeek) {
                if (targetType == MySQLType.ENUM
                        || targetType == MySQLType.CHAR
                        || targetType == MySQLType.VARCHAR) {
                    bindType = targetType;
                } else {
                    bindType = MySQLType.TINYINT;
                }
            } else if (nonNull instanceof ZoneOffset) {
                bindType = MySQLType.INT;
            } else {
                throw MySQLExceptions.createWrongArgumentsException(stmtIndex, meta.sqlType, paramValue, null);
            }
        } else if (nonNull instanceof TemporalAmount) {
            if (nonNull instanceof Duration) {
                bindType = MySQLType.TIME;
            } else {
                throw MySQLExceptions.createWrongArgumentsException(stmtIndex, meta.sqlType, paramValue, null);
            }
        } else if (nonNull instanceof Enum) {
            if (targetType == MySQLType.ENUM
                    || targetType == MySQLType.CHAR
                    || targetType == MySQLType.VARCHAR) {
                bindType = targetType;
            } else {
                bindType = MySQLType.CHAR;
            }
        } else if (nonNull instanceof CodeEnum
                || nonNull instanceof ZoneId) {
            bindType = MySQLType.INT;
        } else if (nonNull instanceof Set) {
            bindType = MySQLType.VARCHAR;
        } else {
            throw MySQLExceptions.createWrongArgumentsException(stmtIndex, meta.sqlType, paramValue, null);
        }
        return bindType;
    }


    /**
     * @see #createExecutePacketBuffer(int)
     * @see #decideBindType(int, MySQLColumnMeta, ParamValue)
     */
    private void bindParameter(ByteBuf buffer, int stmtIndex, final MySQLType bindType, MySQLColumnMeta meta
            , ParamValue paramValue)
            throws SQLException {

        switch (bindType) {
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
                bindToStringType(buffer, stmtIndex, meta, paramValue);
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
            case BIT:
            case SET:
                // here bug.
                throw new IllegalStateException(
                        String.format("MySQL %s type bind must convert by java type.", bindType));
            case NULL:
            case UNKNOWN:
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, meta.sqlType, paramValue);
            default:
                throw MySQLExceptions.createUnexpectedEnumException(meta.sqlType);
        }
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     */
    private void bindToInt1(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNull = bindValue.getNonNull();
        final int int1;
        if (nonNull instanceof Byte) {
            int1 = (Byte) nonNull;
        } else if (nonNull instanceof Boolean) {
            int1 = (Boolean) nonNull ? 1 : 0;
        } else if (nonNull instanceof Month) {
            int1 = ((Month) nonNull).getValue();
        } else if (nonNull instanceof DayOfWeek) {
            int1 = ((DayOfWeek) nonNull).getValue();
        } else if (nonNull instanceof String) {
            Boolean b = MySQLConvertUtils.tryConvertToBoolean((String) nonNull);
            if (b == null) {
                try {
                    if (parameterMeta.sqlType == MySQLType.TINYINT_UNSIGNED) {
                        int1 = Short.parseShort((String) nonNull);
                    } else {
                        int1 = Byte.parseByte((String) nonNull);
                    }
                } catch (NumberFormatException e) {
                    throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.sqlType, bindValue);
                }
            } else {
                int1 = b ? 1 : 0;
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.sqlType, bindValue);
        }
        Packets.writeInt1(buffer, int1);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     */
    private void bindInt2(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNull();
        final int int2;
        if (nonNullValue instanceof Year) {
            int2 = ((Year) nonNullValue).getValue();
        } else if (nonNullValue instanceof Short) {
            int2 = (Short) nonNullValue;
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.sqlType, bindValue);
        }
        Packets.writeInt2(buffer, int2);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value">Binary Protocol Value</a>
     */
    private void bindToDecimal(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue paramValue) {
        final Object nonNullValue = paramValue.getNonNull();
        final String decimal;
        if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            decimal = num.toPlainString();
        } else if (nonNullValue instanceof BigInteger) {
            BigInteger num = (BigInteger) nonNullValue;
            decimal = num.toString();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.sqlType, paramValue);
        }
        Packets.writeStringLenEnc(buffer, decimal.getBytes(this.adjutant.charsetClient()));
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value">Binary Protocol Value</a>
     */
    private void bindToInt4(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue paramValue) {
        final Object nonNull = paramValue.getNonNull();
        final int int4;
        if (nonNull instanceof Integer) {
            int4 = (Integer) nonNull;
        } else if (nonNull instanceof CodeEnum) {
            int4 = ((CodeEnum) nonNull).code();
        } else if (nonNull instanceof ZoneOffset) {
            int4 = ((ZoneOffset) nonNull).getTotalSeconds();
        } else if (nonNull instanceof ZoneId) {
            int4 = MySQLTimes.toZoneOffset((ZoneId) nonNull).getTotalSeconds();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.sqlType, paramValue);
        }
        Packets.writeInt4(buffer, int4);
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     */
    private void bindToFloat(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue bindValue) {
        final Object nonNullValue = bindValue.getNonNull();
        if (nonNullValue instanceof Float) {
            Packets.writeInt4(buffer, Float.floatToIntBits((Float) nonNullValue));
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.sqlType, bindValue);
        }

    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     */
    private void bindToInt8(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue bindValue) {
        final Object nonNull = bindValue.getNonNull();
        final long int8;
        if (nonNull instanceof Long) {
            int8 = (Long) nonNull;
        } else if (nonNull instanceof Instant) {
            int8 = ((Instant) nonNull).getEpochSecond();
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.sqlType, bindValue);
        }
        Packets.writeInt8(buffer, int8);
    }


    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     */
    private void bindToDouble(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNull = bindValue.getNonNull();
        if (nonNull instanceof Double) {
            Packets.writeInt8(buffer, Double.doubleToLongBits((Double) nonNull));
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.sqlType, bindValue);
        }
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value">ProtocolBinary::MYSQL_TYPE_TIME</a>
     */
    private void bindToTime(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNull = bindValue.getNonNull();

        final int microPrecision = parameterMeta.obtainDateTimeTypePrecision();
        final int length = microPrecision > 0 ? 12 : 8;

        if (nonNull instanceof Duration) {
            final Duration duration = (Duration) nonNull;
            if (!MySQLTimes.canConvertToTimeType(duration)) {
                throw MySQLExceptions.createDurationRangeException(stmtIndex, parameterMeta.sqlType, bindValue);
            }
            buffer.writeByte(length); //1. length
            buffer.writeByte(duration.isNegative() ? 1 : 0); //2. is_negative

            long totalSeconds = Math.abs(duration.getSeconds());
            Packets.writeInt4(buffer, (int) (totalSeconds / (3600 * 24))); //3. days
            totalSeconds %= (3600 * 24);

            buffer.writeByte((int) (totalSeconds / 3600)); //4. hour
            totalSeconds %= 3600;

            buffer.writeByte((int) (totalSeconds / 60)); //5. minute
            totalSeconds %= 60;

            buffer.writeByte((int) totalSeconds); //6. second
            if (length == 12) {
                //7, micro seconds
                Packets.writeInt4(buffer, truncateMicroSeconds(duration.getNano() / 1000, microPrecision));
            }
            return;
        }

        final LocalTime time;
        if (nonNull instanceof LocalTime) {
            time = OffsetTime.of((LocalTime) nonNull, this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } else if (nonNull instanceof OffsetTime) {
            time = ((OffsetTime) nonNull).withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } else if (nonNull instanceof String) {
            String timeText = (String) nonNull;
            try {
                time = OffsetTime.of(LocalTime.parse(timeText, MySQLTimes.MYSQL_TIME_FORMATTER)
                        , this.adjutant.obtainZoneOffsetClient())
                        .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                        .toLocalTime();
            } catch (DateTimeParseException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.sqlType, bindValue, e);
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.sqlType, bindValue);
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
                Packets.writeInt4(buffer
                        , truncateMicroSeconds(time.get(ChronoField.MICRO_OF_SECOND), microPrecision));
            }
        }

    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     */
    private void bindToDate(final ByteBuf buffer, int stmtIndex, MySQLColumnMeta columnMeta, ParamValue bindValue) {
        final Object nonNull = bindValue.getNonNull();

        final LocalDate date;
        if (nonNull instanceof LocalDate) {
            date = (LocalDate) nonNull;
        } else if (nonNull instanceof YearMonth) {
            YearMonth yearMonth = (YearMonth) nonNull;
            date = LocalDate.of(yearMonth.getYear(), yearMonth.getMonth(), 1);
        } else if (nonNull instanceof MonthDay) {
            MonthDay monthDay = (MonthDay) nonNull;
            date = LocalDate.of(1970, monthDay.getMonth(), monthDay.getDayOfMonth());
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, columnMeta.sqlType, bindValue);
        }
        buffer.writeByte(4); // length
        Packets.writeInt2(buffer, date.getYear()); // year
        buffer.writeByte(date.getMonthValue()); // month
        buffer.writeByte(date.getDayOfMonth()); // day
    }

    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     */
    private void bindToDatetime(final ByteBuf buffer, int stmtIndex, final MySQLColumnMeta parameterMeta
            , final ParamValue bindValue) {
        final Object nonNull = bindValue.getNonNull();

        final LocalDateTime dateTime;
        if (nonNull instanceof LocalDateTime) {
            dateTime = OffsetDateTime.of((LocalDateTime) nonNull, this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNull instanceof ZonedDateTime) {
            dateTime = ((ZonedDateTime) nonNull)
                    .withZoneSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNull instanceof OffsetDateTime) {
            dateTime = ((OffsetDateTime) nonNull)
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNull instanceof String) {
            try {
                LocalDateTime localDateTime = LocalDateTime.parse((String) nonNull
                        , MySQLTimes.MYSQL_DATETIME_FORMATTER);
                dateTime = OffsetDateTime.of(localDateTime, this.adjutant.obtainZoneOffsetClient())
                        .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                        .toLocalDateTime();
            } catch (DateTimeParseException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.sqlType, bindValue, e);
            }
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, parameterMeta.sqlType, bindValue);
        }

        final int microPrecision = parameterMeta.obtainDateTimeTypePrecision();
        buffer.writeByte(microPrecision > 0 ? 11 : 7); // length
        Packets.writeInt2(buffer, dateTime.getYear()); // year
        buffer.writeByte(dateTime.getMonthValue()); // month
        buffer.writeByte(dateTime.getDayOfMonth()); // day

        buffer.writeByte(dateTime.getHour()); // hour
        buffer.writeByte(dateTime.getMinute()); // minute
        buffer.writeByte(dateTime.getSecond()); // second

        if (microPrecision > 0) {
            // micro second
            Packets.writeInt4(buffer
                    , truncateMicroSeconds(dateTime.get(ChronoField.MICRO_OF_SECOND), microPrecision));
        }

    }


    /**
     * @see #bindParameter(ByteBuf, int, MySQLType, MySQLColumnMeta, ParamValue)
     */
    private void bindToStringType(final ByteBuf buffer, final int stmtIndex, final MySQLColumnMeta meta
            , final ParamValue bindValue) {
        final Object nonNull = bindValue.getNonNull();
        if (nonNull instanceof CharSequence || nonNull instanceof Character) {
            Packets.writeStringLenEnc(buffer, nonNull.toString().getBytes(this.adjutant.charsetClient()));
        } else if (nonNull instanceof byte[]) {
            Packets.writeStringLenEnc(buffer, (byte[]) nonNull);
        } else if (nonNull instanceof Enum) {
            Packets.writeStringLenEnc(buffer, ((Enum<?>) nonNull).name()
                    .getBytes(this.adjutant.charsetClient()));
        } else if (nonNull instanceof Set) {
            Set<?> set = (Set<?>) nonNull;
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
                    throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.sqlType, bindValue);
                }
                index++;
            }
            Packets.writeStringLenEnc(buffer, builder.toString().getBytes(this.adjutant.charsetClient()));
        } else {
            throw MySQLExceptions.createTypeNotMatchException(stmtIndex, meta.sqlType, bindValue);
        }

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
                newMicroSeconds = (microSeconds / 100000) * 100000;
                break;
            case 2:
                newMicroSeconds = (microSeconds / 10000) * 10000;
                break;
            case 3:
                newMicroSeconds = (microSeconds / 1000) * 1000;
                break;
            case 4:
                newMicroSeconds = (microSeconds / 100) * 100;
                break;
            case 5:
                newMicroSeconds = (microSeconds / 10) * 10;
                break;
            case 6:
                newMicroSeconds = microSeconds;
                break;
            default:
                throw new IllegalArgumentException(String.format("precision[%s] not in [0,6]", precision));
        }
        return newMicroSeconds;
    }


    /*################################## blow private static convert method ##################################*/


}
