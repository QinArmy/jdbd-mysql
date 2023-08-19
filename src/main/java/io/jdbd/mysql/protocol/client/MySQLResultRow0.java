package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.UnsupportedConvertingException;
import io.jdbd.vendor.result.AbstractResultRow;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

abstract class MySQLResultRow0 extends AbstractResultRow<MySQLRowMeta> {

    static MySQLResultRow0 from(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
        return new SimpleMySQLResultRow(columnValues, rowMeta, adjutant);
    }

    private final ResultRowAdjutant adjutant;

    private MySQLResultRow0(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
        super(rowMeta, columnValues);
        this.adjutant = adjutant;
    }


    /*################################## blow protected method ##################################*/


    @Override
    protected final boolean needParse(final int indexBaseZero, @Nullable final Class<?> columnClass) {
        return false;
    }

    @Override
    protected final Object parseColumn(final int indexBaseZero, final Object nonNull, @Nullable final Class<?> columnClass) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> Set<T> convertNonNullToSet(final int indexBaseZero, final Object nonValue, final Class<T> elementClass)
            throws UnsupportedConvertingException {
        final Set<T> set;
        if (nonValue instanceof Set) {
            if (this.rowMeta.columnMetaArray[indexBaseZero].sqlType == MySQLType.SET) {
                Set<String> stringSet = (Set<String>) nonValue;
                if (elementClass == String.class) {
                    set = (Set<T>) stringSet;
                } else if (elementClass.isEnum()) {
                    Set<T> tempSet = MySQLStrings.convertStringsToEnumSet(stringSet, elementClass);
                    set = MySQLCollections.unmodifiableSet(tempSet);
                } else {
                    throw createNotSupportedException(indexBaseZero, elementClass);
                }
            } else {
                throw createNotSupportedException(indexBaseZero, elementClass);
            }
        } else {
            set = super.convertNonNullToSet(indexBaseZero, nonValue, elementClass);
        }
        return set;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> List<T> convertNonNullToList(final int indexBaseZero, final Object nonNull
            , final Class<T> elementClass)
            throws UnsupportedConvertingException {
        final List<T> list;
        if (nonNull instanceof Set) {
            if (this.rowMeta.columnMetaArray[indexBaseZero].isSetType()) {
                if (elementClass == String.class) {
                    list = (List<T>) MySQLCollections.unmodifiableList(new ArrayList<>((Set<String>) nonNull));
                } else if (elementClass.isEnum()) {
                    List<T> tempList = MySQLStrings.convertStringsToEnumList((Set<String>) nonNull, elementClass);
                    list = MySQLCollections.unmodifiableList(tempList);
                } else {
                    throw createNotSupportedException(indexBaseZero, elementClass);
                }
            } else {
                throw createNotSupportedException(indexBaseZero, elementClass);
            }
        } else {
            list = super.convertNonNullToList(indexBaseZero, nonNull, elementClass);
        }
        return list;
    }



    @Override
    protected String convertToString(final int indexBaseZero, final Object nonNull) {
        final String text;
        if (nonNull instanceof Long && this.rowMeta.getMySQLType(indexBaseZero) == MySQLType.BIT) {
            text = Long.toBinaryString((Long) nonNull);
        } else {
            text = super.convertToString(indexBaseZero, nonNull);
        }
        return text;
    }


    @Override
    protected Charset obtainColumnCharset(final int indexBaseZero) {
        return this.adjutant.columnCharset(this.rowMeta.getColumnCharset(indexBaseZero));
    }



    @Override
    protected UnsupportedConvertingException createNotSupportedException(final int indexBasedZero
            , final Class<?> targetClass) {
        MySQLType mySQLType = this.rowMeta.getMySQLType(indexBasedZero);

        String message = String.format("Not support convert from (index[%s] alias[%s] and MySQLType[%s]) to %s.",
                indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , mySQLType, targetClass.getName());

        return new UnsupportedConvertingException(message, mySQLType, targetClass);
    }

    @Override
    protected UnsupportedConvertingException createValueCannotConvertException(Throwable cause
            , int indexBasedZero, Class<?> targetClass) {
        MySQLType mySQLType = this.rowMeta.getMySQLType(indexBasedZero);

        String f = "Cannot convert value from (index[%s] alias[%s] and MySQLType[%s]) to %s, please check value rang.";
        String m = String.format(f
                , indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , mySQLType, targetClass.getName());

        return new UnsupportedConvertingException(m, cause, mySQLType, targetClass);
    }



    /*################################## blow private method ##################################*/


    private static final class SimpleMySQLResultRow extends MySQLResultRow0 {

        private SimpleMySQLResultRow(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
            super(columnValues, rowMeta, adjutant);
        }


    }


}
