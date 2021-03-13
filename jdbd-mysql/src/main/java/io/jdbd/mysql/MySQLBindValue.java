package io.jdbd.mysql;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.client.MySQLType;


public final class MySQLBindValue implements BindValue {

    public static MySQLBindValue create(int parameterIndex, MySQLType type, @Nullable Object value) {
        if (parameterIndex < 0) {
            throw new IllegalArgumentException(String.format("parameterIndex[%s]", parameterIndex));
        }
        return new MySQLBindValue(parameterIndex, type, value);
    }

    public static MySQLBindValue create(BindValue bindValue, MySQLType newType) {
        return new MySQLBindValue(bindValue.getParamIndex(), newType, bindValue.getValue());
    }

    private final int parameterIndex;

    private final MySQLType type;

    private final Object value;

    private MySQLBindValue(int parameterIndex, MySQLType type, @Nullable Object value) {
        this.parameterIndex = parameterIndex;
        this.type = type;
        this.value = value;
    }

    @Override
    public MySQLType getType() {
        return this.type;
    }

    @Override
    public int getParamIndex() {
        return this.parameterIndex;
    }

    @Override
    public boolean isStream() {
        return false;
    }

    @Nullable
    @Override
    public Object getValue() {
        return this.value;
    }

    @Override
    public Object getRequiredValue() throws NullPointerException {
        Object value = this.value;
        if (value == null) {
            throw new NullPointerException(String.format("Bind parameter[%s] value is null.", this.parameterIndex));
        }
        return value;
    }
}
