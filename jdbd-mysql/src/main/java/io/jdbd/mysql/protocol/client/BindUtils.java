package io.jdbd.mysql.protocol.client;

import io.jdbd.BindParameterException;
import io.jdbd.MultiResults;
import io.jdbd.mysql.BindValue;
import io.jdbd.mysql.syntax.MySQLStatement;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.jdbd.vendor.result.JdbdMultiResults;
import io.jdbd.vendor.util.JdbdBindUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.List;

abstract class BindUtils extends JdbdBindUtils {

    protected BindUtils() {
        throw new UnsupportedOperationException();
    }

    static <T> Flux<T> createParamCountNotMatchFluxError(MySQLStatement stmt, List<BindValue> parameterGroup) {
        Flux<T> flux;
        final int bindCount = parameterGroup.size(), paramCount = stmt.getParamCount();
        if (paramCount == 0) {
            flux = Flux.error(MySQLExceptions.createNoParametersExistsException());
        } else if (paramCount > bindCount) {
            flux = Flux.error(MySQLExceptions.createParamsNotBindException(bindCount));
        } else {
            flux = Flux.error(MySQLExceptions.createInvalidParameterException(paramCount));
        }
        return flux;
    }

    static <T> Mono<T> createParamCountNotMatchMonoError(MySQLStatement stmt, List<BindValue> parameterGroup) {
        Mono<T> mono;
        final int bindCount = parameterGroup.size(), paramCount = stmt.getParamCount();
        if (paramCount == 0) {
            mono = Mono.error(MySQLExceptions.createNoParametersExistsException());
        } else if (paramCount > bindCount) {
            mono = Mono.error(MySQLExceptions.createParamsNotBindException(bindCount));
        } else {
            mono = Mono.error(MySQLExceptions.createInvalidParameterException(paramCount));
        }
        return mono;
    }

    static MultiResults createParamCountNotMatchError(MySQLStatement stmt, List<BindValue> parameterGroup
            , int stmtIndex) {
        MultiResults results;
        final int bindCount = parameterGroup.size(), paramCount = stmt.getParamCount();
        if (paramCount == 0) {
            results = JdbdMultiResults.error(MySQLExceptions.createNoParametersExistsException(stmtIndex));
        } else if (paramCount > bindCount) {
            results = JdbdMultiResults.error(MySQLExceptions.createParamsNotBindException(stmtIndex, bindCount));
        } else {
            results = JdbdMultiResults.error(MySQLExceptions.createInvalidParameterException(stmtIndex, paramCount));
        }
        return results;
    }


    public static String bindToBits(final int stmtIndex, final BindValue bindValue) throws SQLException {
        final Object nonNullValue = bindValue.getRequiredValue();

        final String bits;
        if (nonNullValue instanceof Long) {
            bits = Long.toBinaryString((Long) nonNullValue);
        } else if (nonNullValue instanceof Integer
                || nonNullValue instanceof Short
                || nonNullValue instanceof Byte) {
            bits = Integer.toBinaryString(((Number) nonNullValue).intValue());
        } else if (nonNullValue instanceof byte[]) {
            final byte[] bytes = (byte[]) nonNullValue;
            StringBuilder builder = new StringBuilder(bytes.length * 8);
            for (byte b : bytes) {
                for (int i = 0; i < 8; i++) {
                    builder.append((b & (1 << i)) != 0 ? 1 : 0);
                }
            }
            bits = builder.toString();
        } else if (nonNullValue instanceof String) {
            bits = (String) nonNullValue;
            if (!MySQLStringUtils.isBinaryString(bits)) {
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
            }
        } else if (nonNullValue instanceof BigInteger) {
            bits = ((BigInteger) nonNullValue).toString(2);
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) nonNullValue;
            if (decimal.scale() != 0) {
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
            }
            bits = decimal.toPlainString();
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }

        return ("B'" + bits + "'");
    }


    static DateTimeFormatter obtainTimeFormatter(final int microPrecision) {
        final DateTimeFormatter formatter;
        switch (microPrecision) {
            case 0:
                formatter = MySQLTimeUtils.MYSQL_TIME_FORMATTER_0;
                break;
            case 6:
                formatter = MySQLTimeUtils.MYSQL_TIME_FORMATTER;
                break;
            case 1:
                formatter = MySQLTimeUtils.MYSQL_TIME_FORMATTER_1;
                break;
            case 2:
                formatter = MySQLTimeUtils.MYSQL_TIME_FORMATTER_2;
                break;
            case 3:
                formatter = MySQLTimeUtils.MYSQL_TIME_FORMATTER_3;
                break;
            case 4:
                formatter = MySQLTimeUtils.MYSQL_TIME_FORMATTER_4;
                break;
            case 5:
                formatter = MySQLTimeUtils.MYSQL_TIME_FORMATTER_5;
                break;
            default:
                throw new IllegalArgumentException(String.format("microPrecision[%s] error", microPrecision));

        }

        return formatter;
    }

    static DateTimeFormatter obtainDateTimeFormatter(final int microPrecision) {
        final DateTimeFormatter formatter;

        switch (microPrecision) {
            case 0:
                formatter = MySQLTimeUtils.MYSQL_DATETIME_FORMATTER_0;
                break;
            case 6:
                formatter = MySQLTimeUtils.MYSQL_DATETIME_FORMATTER;
                break;
            case 1:
                formatter = MySQLTimeUtils.MYSQL_DATETIME_FORMATTER_1;
                break;
            case 2:
                formatter = MySQLTimeUtils.MYSQL_DATETIME_FORMATTER_2;
                break;
            case 3:
                formatter = MySQLTimeUtils.MYSQL_DATETIME_FORMATTER_3;
                break;
            case 4:
                formatter = MySQLTimeUtils.MYSQL_DATETIME_FORMATTER_4;
                break;
            case 5:
                formatter = MySQLTimeUtils.MYSQL_DATETIME_FORMATTER_5;
                break;
            default:
                throw new IllegalArgumentException(String.format("microPrecision[%s] error.", microPrecision));

        }

        return formatter;

    }


    /*################################## blow private exception ##################################*/

    static BindParameterException createTypeNotMatchException(BindValue bindValue) {
        return createTypeNotMatchException(bindValue, null);
    }

    static BindParameterException createTypeNotMatchException(BindValue bindValue, @Nullable Throwable cause) {
        return new BindParameterException(cause, bindValue.getParamIndex()
                , "Bind parameter[%s] MySQLType[%s] and JavaType[%s] value not match."
                , bindValue.getParamIndex()
                , bindValue.getType()
                , bindValue.getRequiredValue().getClass().getName()
        );
    }

    static BindParameterException createNotSupportFractionException(BindValue bindValue) {
        throw new BindParameterException(String.format("Bind parameter[%s] is MySQLType[%s],not support fraction."
                , bindValue.getParamIndex()
                , bindValue.getType())
                , bindValue.getParamIndex());
    }

    static BindParameterException createNumberRangErrorException(BindValue bindValue, Number lower
            , Number upper) {
        return new BindParameterException(String.format("Bind parameter[%s] MySQLType[%s] beyond rang[%s,%s]."
                , bindValue.getParamIndex(), bindValue.getType(), lower, upper)
                , bindValue.getParamIndex());

    }


}
