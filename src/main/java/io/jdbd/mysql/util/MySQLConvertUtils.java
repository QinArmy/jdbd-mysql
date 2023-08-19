package io.jdbd.mysql.util;

import io.jdbd.result.NotSupportedConvertException;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class MySQLConvertUtils {

    protected MySQLConvertUtils() {
        throw new UnsupportedOperationException();
    }


    /**
     * @throws IllegalArgumentException throw when convert failure.
     */
    public static Boolean convertObjectToBoolean(Object nonNull) {
        Boolean boolValue;
        if (nonNull instanceof String) {
            boolValue = tryConvertToBoolean((String) nonNull);
            if (boolValue == null) {
                BigDecimal decimal;
                try {
                    decimal = new BigDecimal((String) nonNull);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Can't convert value to boolean,pleas check value range.");
                }
                boolValue = tryConvertToBoolean(decimal);
            }
        } else if (nonNull instanceof Number) {
            boolValue = tryConvertToBoolean((Number) nonNull);
        } else {
            throw new IllegalArgumentException("Can't convert value to boolean,pleas check value type and range.");
        }
        if (boolValue == null) {
            throw new IllegalArgumentException("Can't convert value to boolean,pleas check value type and range.");
        }
        return boolValue;
    }

    @Nullable
    public static Boolean tryConvertToBoolean(String nonNull) {
        Boolean value;
        if (nonNull.equalsIgnoreCase("true")
                || nonNull.equalsIgnoreCase("Y")
                || nonNull.equalsIgnoreCase("T")) {
            value = Boolean.TRUE;

        } else if (nonNull.equalsIgnoreCase("false")
                || nonNull.equalsIgnoreCase("N")
                || nonNull.equalsIgnoreCase("F")) {
            value = Boolean.FALSE;
        } else {
            value = null;
        }
        return value;

    }

    @Nullable
    public static Boolean tryConvertToBoolean(Number nonNull) {
        final Boolean newValue;
        if (nonNull instanceof Long
                || nonNull instanceof Integer
                || nonNull instanceof Byte
                || nonNull instanceof Short) {
            // most probably for mysql
            // Goes back to ODBC driver compatibility, and VB/Automation Languages/COM, where in Windows "-1" can mean true as well.
            // @see io.jdbd.mysql.protocol.client.AbstractClientProtocol.toBoolean
            long l = nonNull.longValue();
            newValue = (l == -1 || l > 0);
        } else if (nonNull instanceof Float || nonNull instanceof Double) {
            //this means that 0.1 or -1 will be TRUE
            // @see io.jdbd.mysql.protocol.client.AbstractClientProtocol.toBoolean
            double d = nonNull.doubleValue();
            newValue = d > 0 || d == -1.0d;
        } else if (nonNull instanceof BigDecimal) {
            //this means that 0.1 or -1 will be TRUE
            BigDecimal d = (BigDecimal) nonNull;
            newValue = d.compareTo(BigDecimal.ZERO) > 0 || d.compareTo(BigDecimal.valueOf(-1L)) == 0;
        } else if (nonNull instanceof BigInteger) {
            BigInteger i = (BigInteger) nonNull;
            newValue = i.compareTo(BigInteger.ZERO) > 0 || i.compareTo(BigInteger.valueOf(-1L)) == 0;
        } else {
            newValue = null;
        }
        return newValue;
    }

    /**
     * @return unmodifiable {@link Set}
     */
    public static Set<String> convertToSetType(final String nonNull) {
        String[] array = nonNull.split(",");
        Set<String> set;
        if (array.length == 0) {
            set = Collections.emptySet();
        } else {
            set = new HashSet<>((int) (array.length / 0.75F));
            for (String s : array) {
                set.add(s.trim());
            }
            set = Collections.unmodifiableSet(set);
        }
        return set;
    }

    /*################################## blow private method ##################################*/

    private static NotSupportedConvertException createNotSupportedException(Object value, Class<?> targetClass
            , @Nullable Throwable e) {
        String m = String.format("Not support convert from value[%s] and type[%s] to [%s] ."
                , value
                , value.getClass().getName(), targetClass.getName());
        return new NotSupportedConvertException(m, e);
    }


}
