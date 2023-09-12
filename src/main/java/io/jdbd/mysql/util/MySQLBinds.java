package io.jdbd.mysql.util;

import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.meta.UserDefinedType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.vendor.stmt.Value;
import io.jdbd.vendor.util.JdbdBinds;
import io.jdbd.vendor.util.JdbdCollections;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;

import java.time.Year;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public abstract class MySQLBinds extends JdbdBinds {

    private MySQLBinds() {
    }

    //TODO add alias mapping
    public static final Map<String, MySQLType> MYSQL_TYPE_MAP = createMySqlTypeMap();

    // private static final Logger LOG = LoggerFactory.getLogger(MySQLBinds.class);


    public static MySQLType mapDataType(final DataType dataType) {
        final MySQLType type;
        if (dataType instanceof MySQLType) {
            type = (MySQLType) dataType;
        } else if (!(dataType instanceof JdbdType)) {
            if (dataType instanceof UserDefinedType) { // mysql don't support user defined type.
                type = MySQLType.UNKNOWN;
            } else {
                type = MYSQL_TYPE_MAP.getOrDefault(dataType.typeName().toUpperCase(Locale.ROOT), MySQLType.UNKNOWN);
            }
        } else switch ((JdbdType) dataType) {
            case NULL:
                type = MySQLType.NULL;
                break;
            case BOOLEAN:
                type = MySQLType.BOOLEAN;
                break;
            case BIT:
            case VARBIT:
                type = MySQLType.BIT;
                break;

            case TINYINT:
                type = MySQLType.TINYINT;
                break;
            case SMALLINT:
                type = MySQLType.SMALLINT;
                break;
            case MEDIUMINT:
                type = MySQLType.MEDIUMINT;
                break;
            case INTEGER:
                type = MySQLType.INT;
                break;
            case BIGINT:
                type = MySQLType.BIGINT;
                break;
            case DECIMAL:
            case NUMERIC:
                type = MySQLType.DECIMAL;
                break;
            case FLOAT:
            case REAL:
                type = MySQLType.FLOAT;
                break;
            case DOUBLE:
                type = MySQLType.DOUBLE;
                break;

            case TINYINT_UNSIGNED:
                type = MySQLType.TINYINT_UNSIGNED;
                break;
            case SMALLINT_UNSIGNED:
                type = MySQLType.SMALLINT_UNSIGNED;
                break;
            case MEDIUMINT_UNSIGNED:
                type = MySQLType.MEDIUMINT_UNSIGNED;
                break;
            case INTEGER_UNSIGNED:
                type = MySQLType.INT_UNSIGNED;
                break;
            case BIGINT_UNSIGNED:
                type = MySQLType.BIGINT_UNSIGNED;
                break;
            case DECIMAL_UNSIGNED:
                type = MySQLType.DECIMAL_UNSIGNED;
                break;

            case TIME:
            case TIME_WITH_TIMEZONE:
            case DURATION:
                type = MySQLType.TIME;
                break;
            case YEAR:
                type = MySQLType.YEAR;
                break;
            case YEAR_MONTH:
            case MONTH_DAY:
            case DATE:
                type = MySQLType.DATE;
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                type = MySQLType.DATETIME;
                break;

            case BINARY:
                type = MySQLType.BINARY;
                break;
            case VARBINARY:
                type = MySQLType.VARBINARY;
                break;
            case TINYBLOB:
                type = MySQLType.TINYBLOB;
                break;
            case MEDIUMBLOB:
                type = MySQLType.MEDIUMBLOB;
                break;
            case BLOB:
                type = MySQLType.BLOB;
                break;
            case LONGBLOB:
                type = MySQLType.LONGBLOB;
                break;

            case CHAR:
                type = MySQLType.CHAR;
                break;
            case VARCHAR:
                type = MySQLType.VARCHAR;
                break;
            case ENUM:
                type = MySQLType.ENUM;
                break;
            case TINYTEXT:
                type = MySQLType.TINYTEXT;
                break;
            case MEDIUMTEXT:
                type = MySQLType.MEDIUMTEXT;
                break;
            case TEXT:
                type = MySQLType.TEXT;
                break;
            case LONGTEXT:
                type = MySQLType.LONGTEXT;
                break;

            case JSON:
            case JSONB:
                type = MySQLType.JSON;
                break;

            case GEOMETRY:
                type = MySQLType.GEOMETRY;
                break;
            case XML:
            case ARRAY:
            case ROWID:
            case PERIOD:
            case INTERVAL:
            case DIALECT_TYPE:
            case UNKNOWN:
            case REF_CURSOR:
            case USER_DEFINED:
            case COMPOSITE:
            case INTERNAL_USE:
            default:
                type = MySQLType.UNKNOWN;
        }
        return type;
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/year.html">The YEAR Type</a>
     */
    public static int bindToYear(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNull();
        final int value;
        if (nonNull instanceof Year) {
            value = ((Year) nonNull).getValue();
        } else if (nonNull instanceof Short) {
            value = (Short) nonNull;
        } else if (nonNull instanceof Integer) {
            value = (Integer) nonNull;
        } else {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
        }

        if (value > 2155 || (value < 1901 && value != 0)) {
            throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
        }
        return value;
    }

    public static String bindToSetType(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNull();
        if (nonNull instanceof String) {
            return (String) nonNull;
        }
        if (!(nonNull instanceof Set)) {
            throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        final Set<?> set = (Set<?>) nonNull;
        final StringBuilder builder = new StringBuilder(set.size() * 4);
        int index = 0;
        for (Object element : set) {
            if (index > 0) {
                builder.append(Constants.COMMA);
            }
            if (element instanceof String) {
                builder.append(element);
            } else if (element instanceof Enum) {
                builder.append(((Enum<?>) element).name());
            } else {
                throw JdbdExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
            }
            index++;
        }
        return builder.toString();
    }


    public static void assertParamCountMatch(int stmtIndex, int paramCount, int bindCount) {

        if (bindCount != paramCount) {
            if (paramCount == 0) {
                throw MySQLExceptions.createNoParametersExistsError(stmtIndex);
            } else if (paramCount > bindCount) {
                throw MySQLExceptions.createParamsNotBindError(stmtIndex, bindCount);
            } else {
                throw MySQLExceptions.createInvalidParameterNoError(stmtIndex, paramCount);
            }
        }
    }

    public static void releaseOnError(Queue<ByteBuf> queue, final ByteBuf packet) {
        ByteBuf byteBuf;
        while ((byteBuf = queue.poll()) != null) {
            byteBuf.release();
        }
        queue.clear();
        if (packet.refCnt() > 0) {
            packet.release();
        }
    }


    private static Map<String, MySQLType> createMySqlTypeMap() {
        final MySQLType[] valueArray = MySQLType.values();
        final Map<String, MySQLType> map = JdbdCollections.hashMap((int) ((valueArray.length + 18) / 0.75f));
        for (MySQLType value : valueArray) {
            map.put(value.typeName(), value);
        }

        map.put("INTEGER", MySQLType.INT);
        map.put("INTEGER UNSIGNED", MySQLType.INT_UNSIGNED);
        map.put("DEC", MySQLType.DECIMAL);
        map.put("DEC UNSIGNED", MySQLType.DECIMAL_UNSIGNED);

        map.put("NUMERIC", MySQLType.DECIMAL);
        map.put("NUMERIC UNSIGNED", MySQLType.DECIMAL_UNSIGNED);
        map.put("DOUBLE PRECISION", MySQLType.DECIMAL);
        map.put("DOUBLE PRECISION UNSIGNED", MySQLType.DECIMAL_UNSIGNED);

        map.put("POINT", MySQLType.GEOMETRY);
        map.put("LINESTRING", MySQLType.GEOMETRY);
        map.put("LINE", MySQLType.GEOMETRY);
        map.put("LINEARRING", MySQLType.GEOMETRY);

        map.put("POLYGON", MySQLType.GEOMETRY);
        map.put("MULTIPOINT", MySQLType.GEOMETRY);
        map.put("MULTILINESTRING", MySQLType.GEOMETRY);
        map.put("MULTIPOLYGON", MySQLType.GEOMETRY);

        map.put("GEOMCOLLECTION", MySQLType.GEOMETRY);
        map.put("GEOMETRYCOLLECTION", MySQLType.GEOMETRY);
        return MySQLCollections.unmodifiableMap(map);
    }

}
