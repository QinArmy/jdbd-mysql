package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.type.PathParameter;
import io.jdbd.vendor.result.ColumnConverts;
import io.jdbd.vendor.result.ColumnMeta;
import io.jdbd.vendor.result.VendorDataRow;
import io.jdbd.vendor.util.JdbdExceptions;
import org.reactivestreams.Publisher;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;

abstract class MySQLRow extends VendorDataRow {

    final MySQLRowMeta rowMeta;

    final Object[] columnArray;


    private MySQLRow(MySQLRowMeta rowMeta) {
        this.rowMeta = rowMeta;
        final int arrayLength;
        arrayLength = rowMeta.columnMetaArray.length;
        this.columnArray = new Object[arrayLength];
    }

    private MySQLRow(JdbdCurrentRow currentRow) {
        this.rowMeta = currentRow.rowMeta;

        final Object[] columnArray = new Object[currentRow.columnArray.length];
        System.arraycopy(currentRow.columnArray, 0, columnArray, 0, columnArray.length);

        this.columnArray = columnArray;
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
        return this.columnArray[this.rowMeta.checkIndex(indexBasedZero)] instanceof PathParameter;
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
        final T value;
        if (!(source instanceof Duration)) {
            value = ColumnConverts.convertToTarget(meta, source, columnClass, rowMeta.serverZone);
        } else if (columnClass == String.class) {
            value = (T) MySQLTimes.durationToTimeText((Duration) source);
        } else {
            throw JdbdExceptions.cannotConvertColumnValue(meta, source, columnClass, null);
        }
        return value;
    }


    @Override
    public final <T> List<T> getList(int indexBasedZero, Class<T> elementClass, IntFunction<List<T>> constructor)
            throws JdbdException {
        return null;
    }

    @Override
    public <T> Set<T> getSet(int indexBasedZero, Class<T> elementClass, IntFunction<Set<T>> constructor)
            throws JdbdException {
        return null;
    }

    @Override
    public final <K, V> Map<K, V> getMap(int indexBasedZero, Class<K> keyClass, Class<V> valueClass,
                                         IntFunction<Map<K, V>> constructor) throws JdbdException {
        return null;
    }

    @Override
    public <T> Publisher<T> getPublisher(int indexBasedZero, Class<T> valueClass) throws JdbdException {
        return null;
    }


    @Override
    protected final ColumnMeta getColumnMeta(final int safeIndex) {
        return this.rowMeta.columnMetaArray[safeIndex];
    }

    static abstract class JdbdCurrentRow extends MySQLRow implements CurrentRow {

        JdbdCurrentRow(MySQLRowMeta rowMeta) {
            super(rowMeta);
        }

        @Override
        public final ResultRow asResultRow() {
            return new MySQLResultRow(this);
        }


    }//JdbdCurrentRow

    private static final class MySQLResultRow extends MySQLRow implements ResultRow {

        private final boolean bigRow;

        private MySQLResultRow(JdbdCurrentRow currentRow) {
            super(currentRow);
            this.bigRow = currentRow.isBigRow();
        }

        @Override
        public boolean isBigRow() {
            return this.bigRow;
        }


    }//MySQLResultRow


}
