package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.meta.NullMode;
import io.jdbd.meta.SQLType;
import io.jdbd.result.FieldType;
import io.jdbd.result.ResultRowMeta;
import io.netty.buffer.ByteBuf;

import java.sql.JDBCType;
import java.util.List;

final class PgRowMeta implements ResultRowMeta {

    static PgRowMeta read(ByteBuf message, TaskAdjutant adjutant) {
        PgColumnMeta[] columnMetaArray = PgColumnMeta.read(message, adjutant);
        return columnMetaArray.length == 0 ? EMPTY : new PgRowMeta(columnMetaArray);
    }

    static final PgRowMeta EMPTY = new PgRowMeta(PgColumnMeta.EMPTY);

    final PgColumnMeta[] columnMetaArray;

    private PgRowMeta(PgColumnMeta[] columnMetaArray) {
        this.columnMetaArray = columnMetaArray;
    }


    @Override
    public int getColumnCount() {
        return 0;
    }

    @Override
    public FieldType getFieldType() {
        return null;
    }

    @Override
    public List<String> getColumnAliasList() {
        return null;
    }

    @Override
    public String getColumnLabel(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public int getColumnIndex(String columnLabel) throws JdbdSQLException {
        return 0;
    }

    @Override
    public JDBCType getJdbdType(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public boolean isPhysicalColumn(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public SQLType getSQLType(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public SQLType getSQLType(String columnAlias) throws JdbdSQLException {
        return null;
    }

    @Override
    public NullMode getNullMode(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public NullMode getNullMode(String columnAlias) throws JdbdSQLException {
        return null;
    }

    @Override
    public boolean isUnsigned(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isUnsigned(String columnAlias) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isAutoIncrement(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isAutoIncrement(String columnAlias) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public String getCatalogName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public String getSchemaName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public String getTableName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public String getColumnName(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public boolean isReadOnly(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isWritable(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public Class<?> getColumnClass(int indexBaseZero) throws JdbdSQLException {
        return null;
    }

    @Override
    public long getPrecision(int indexBaseZero) throws JdbdSQLException {
        return 0;
    }

    @Override
    public long getPrecision(String columnAlias) throws JdbdSQLException {
        return 0;
    }

    @Override
    public int getScale(int indexBaseZero) throws JdbdSQLException {
        return 0;
    }

    @Override
    public int getScale(String columnAlias) throws JdbdSQLException {
        return 0;
    }

    @Override
    public boolean isPrimaryKey(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isPrimaryKey(String columnAlias) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isUniqueKey(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isUniqueKey(String columnAlias) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isMultipleKey(int indexBaseZero) throws JdbdSQLException {
        return false;
    }

    @Override
    public boolean isMultipleKey(String columnAlias) throws JdbdSQLException {
        return false;
    }


}