package io.jdbd.mysql;

import io.jdbd.meta.JdbdType;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.type.BlobPath;
import io.jdbd.type.TextPath;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.Set;

/**
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/data-types.html"> Data Types</a>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html#a69e798807026a0f7e12b1d6c72374854">MySQL type identifier</a>
 * @since 1.0
 */
public enum MySQLType implements SQLType {

    /**
     * BOOL, BOOLEAN
     * These types are synonyms for TINYINT(1). A value of zero is considered false. Nonzero values are considered true
     * <p>
     * BOOLEAN is converted to TINYINT(1) during DDL execution i.e. it has the same precision=3. Thus we have to
     * look at full data type name and convert TINYINT to BOOLEAN  if it has "(1)" length specification.
     * <p>
     * Protocol: TYPE_TINY = 1
     */
    BOOLEAN(Constants.TYPE_BOOL, JdbdType.BOOLEAN, Boolean.class),

    /**
     * TINYINT[(M)] [UNSIGNED] [ZEROFILL]
     * A very small integer. The signed range is -128 to 127. The unsigned range is 0 to 255.
     * <p>
     * Protocol: TYPE_TINY = 1
     */
    TINYINT(Constants.TYPE_TINY, JdbdType.TINYINT, Byte.class),

    /**
     * TINYINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see #TINYINT
     */
    TINYINT_UNSIGNED(Constants.TYPE_TINY, true, JdbdType.TINYINT_UNSIGNED, Short.class),


    /**
     * SMALLINT[(M)] [UNSIGNED] [ZEROFILL]
     * A small integer. The signed range is -32768 to 32767. The unsigned range is 0 to 65535.
     * <p>
     * Protocol: TYPE_SHORT = 2
     */
    SMALLINT(Constants.TYPE_SHORT, JdbdType.SMALLINT, Short.class),

    /**
     * SMALLINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see #SMALLINT
     */
    SMALLINT_UNSIGNED(Constants.TYPE_SHORT, true, JdbdType.SMALLINT_UNSIGNED, Integer.class),

    /**
     * MEDIUMINT[(M)] [UNSIGNED] [ZEROFILL]
     * A medium-sized integer. The signed range is -8388608 to 8388607. The unsigned range is 0 to 16777215.
     * <p>
     * Protocol: TYPE_INT24 = 9
     */
    MEDIUMINT(Constants.TYPE_INT24, JdbdType.MEDIUMINT, Integer.class),
    /**
     * MEDIUMINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see #MEDIUMINT
     */
    MEDIUMINT_UNSIGNED(Constants.TYPE_INT24, true, JdbdType.MEDIUMINT_UNSIGNED, Integer.class),

    /**
     * INT[(M)] [UNSIGNED] [ZEROFILL]
     * A normal-size integer. The signed range is -2147483648 to 2147483647. The unsigned range is 0 to 4294967295.
     * <p>
     * Protocol: TYPE_LONG = 3
     * <p>
     * INTEGER[(M)] [UNSIGNED] [ZEROFILL] is a synonym for INT.
     */
    INT(Constants.TYPE_LONG, JdbdType.INTEGER, Integer.class),
    /**
     * INT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see #INT
     */
    INT_UNSIGNED(Constants.TYPE_LONG, true, JdbdType.INTEGER_UNSIGNED, Long.class),


    /**
     * BIGINT[(M)] [UNSIGNED] [ZEROFILL]
     * A large integer. The signed range is -9223372036854775808 to 9223372036854775807. The unsigned range is 0 to 18446744073709551615.
     * <p>
     * Protocol: TYPE_LONGLONG = 8
     * <p>
     * SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
     */
    BIGINT(Constants.TYPE_LONGLONG, JdbdType.BIGINT, Long.class),
    /**
     * BIGINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see #BIGINT
     */
    BIGINT_UNSIGNED(Constants.TYPE_LONGLONG, true, JdbdType.BIGINT_UNSIGNED, BigInteger.class),

    /**
     * DECIMAL[(M[,D])] [UNSIGNED] [ZEROFILL]
     * A packed "exact" fixed-point number. M is the total number of digits (the precision) and D is the number of digits
     * after the decimal point (the scale). The decimal point and (for negative numbers) the "-" sign are not counted in M.
     * If D is 0, values have no decimal point or fractional part. The maximum number of digits (M) for DECIMAL is 65.
     * The maximum number of supported decimals (D) is 30. If D is omitted, the default is 0. If M is omitted, the default is 10.
     * <p>
     * Protocol: TYPE_DECIMAL = 0
     * Protocol: TYPE_NEWDECIMAL = 246
     * <p>
     * These types are synonyms for DECIMAL:
     * DEC[(M[,D])] [UNSIGNED] [ZEROFILL],
     * NUMERIC[(M[,D])] [UNSIGNED] [ZEROFILL],
     * FIXED[(M[,D])] [UNSIGNED] [ZEROFILL]
     */
    DECIMAL(Constants.TYPE_DECIMAL, JdbdType.DECIMAL, BigDecimal.class),
    /**
     * DECIMAL[(M[,D])] UNSIGNED [ZEROFILL]
     *
     * @see #DECIMAL
     */
    DECIMAL_UNSIGNED(Constants.TYPE_DECIMAL, true, JdbdType.DECIMAL_UNSIGNED, BigDecimal.class),

    /**
     * FLOAT[(M,D)] [UNSIGNED] [ZEROFILL]
     * A small (single-precision) floating-point number. Permissible values are -3.402823466E+38 to -1.175494351E-38, 0,
     * and 1.175494351E-38 to 3.402823466E+38. These are the theoretical limits, based on the IEEE standard. The actual
     * range might be slightly smaller depending on your hardware or operating system.
     * <p>
     * M is the total number of digits and D is the number of digits following the decimal point. If M and D are omitted,
     * values are stored to the limits permitted by the hardware. A single-precision floating-point number is accurate to
     * approximately 7 decimal places.
     * <p>
     * Protocol: TYPE_FLOAT = 4
     * <p>
     * Additionally:
     * FLOAT(p) [UNSIGNED] [ZEROFILL]
     * A floating-point number. p represents the precision in bits, but MySQL uses this value only to determine whether
     * to use FLOAT or DOUBLE for the resulting data type. If p is from 0 to 24, the data type becomes FLOAT with no M or D values.
     * If p is from 25 to 53, the data type becomes DOUBLE with no M or D values. The range of the resulting column is the same as
     * for the single-precision FLOAT or double-precision DOUBLE data types.
     */
    FLOAT(Constants.TYPE_FLOAT, JdbdType.FLOAT, Float.class),
    /**
     * FLOAT[(M,D)] UNSIGNED [ZEROFILL]
     * <p>
     * UNSIGNED,disallows negative values. As of MySQL 8.0.17, the UNSIGNED attribute is deprecated
     * </p>
     *
     * @see #FLOAT
     * @deprecated use {@link #FLOAT}, As of MySQL 8.0.17, the UNSIGNED attribute is deprecated for columns of type FLOAT
     * ,see <a href="https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html">Floating-Point Types</a>
     */
    @Deprecated
    FLOAT_UNSIGNED(Constants.TYPE_FLOAT, true, JdbdType.FLOAT, Float.class),
    /**
     * DOUBLE[(M,D)] [UNSIGNED] [ZEROFILL]
     * A normal-size (double-precision) floating-point number. Permissible values are -1.7976931348623157E+308 to
     * -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308. These are the theoretical limits,
     * based on the IEEE standard. The actual range might be slightly smaller depending on your hardware or operating system.
     * <p>
     * M is the total number of digits and D is the number of digits following the decimal point. If M and D are omitted,
     * values are stored to the limits permitted by the hardware. A double-precision floating-point number is accurate to
     * approximately 15 decimal places.
     * <p>
     * Protocol: TYPE_DOUBLE = 5
     * <p>
     * These types are synonyms for DOUBLE:
     * DOUBLE PRECISION[(M,D)] [UNSIGNED] [ZEROFILL],
     * REAL[(M,D)] [UNSIGNED] [ZEROFILL]. Exception: If the REAL_AS_FLOAT SQL mode is enabled, REAL is a synonym for FLOAT rather than DOUBLE.
     */
    DOUBLE(Constants.TYPE_DOUBLE, JdbdType.DOUBLE, Double.class),
    /**
     * DOUBLE[(M,D)] UNSIGNED [ZEROFILL]
     * <p>
     * UNSIGNED, disallows negative values. As of MySQL 8.0.17, the UNSIGNED attribute is deprecated
     * </p>
     *
     * @see #DOUBLE
     * @deprecated use {@link #DOUBLE}, As of MySQL 8.0.17, this syntax is deprecated and you should expect support
     * for it to be removed in a future version of MySQL.
     * see <a href="https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html">Floating-Point Types</a>
     */
    @Deprecated
    DOUBLE_UNSIGNED(Constants.TYPE_DOUBLE, true, JdbdType.DOUBLE, Double.class),

    /**
     * TIME[(fsp)]
     * A time. The range is '-838:59:59.000000' to '838:59:59.000000'. MySQL displays TIME values in
     * 'HH:MM:SS[.fraction]' format, but permits assignment of values to TIME columns using either strings or numbers.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     * <p>
     * Protocol: TYPE_TIME = 11, support bind(or get)  {@link LocalTime} or {@link java.time.Duration}
     *
     * </p>
     */
    TIME(Constants.TYPE_TIME, JdbdType.TIME, Object.class),

    /**
     * DATE
     * A date. The supported range is '1000-01-01' to '9999-12-31'. MySQL displays DATE values in 'YYYY-MM-DD' format,
     * but permits assignment of values to DATE columns using either strings or numbers.
     * <p>
     * Protocol: TYPE_DATE = 10
     */
    DATE(Constants.TYPE_DATE, JdbdType.DATE, LocalDate.class),

    /**
     * YEAR[(4)]
     * A year in four-digit format. MySQL displays YEAR values in YYYY format, but permits assignment of
     * values to YEAR columns using either strings or numbers. Values display as 1901 to 2155, and 0000.
     * Protocol: TYPE_YEAR = 13
     */
    YEAR(Constants.TYPE_YEAR, JdbdType.YEAR, Year.class),

    /**
     * DATETIME[(fsp)]
     * A date and time combination. The supported range is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'.
     * MySQL displays DATETIME values in 'YYYY-MM-DD HH:MM:SS[.fraction]' format, but permits assignment of values to
     * DATETIME columns using either strings or numbers.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     * <p>
     * Protocol: TYPE_DATETIME = 12,support bind below:
     * <ul>
     *     <li>{@link LocalDateTime}</li>
     *     <li>{@link java.time.OffsetDateTime},As of MySQL 8.0.19.</li>
     *     <li>{@link java.time.ZonedDateTime},As of MySQL 8.0.19.</li>
     * </ul>
     * Only support get {@link LocalDateTime}.
     * </p>
     */
    DATETIME(Constants.TYPE_DATETIME, JdbdType.TIMESTAMP, LocalDateTime.class),

    /**
     * TIMESTAMP[(fsp)]
     * A timestamp. The range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999' UTC.
     * TIMESTAMP values are stored as the number of seconds since the epoch ('1970-01-01 00:00:00' UTC).
     * A TIMESTAMP cannot represent the value '1970-01-01 00:00:00' because that is equivalent to 0 seconds
     * from the epoch and the value 0 is reserved for representing '0000-00-00 00:00:00', the "zero" TIMESTAMP value.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     * <p>
     * Protocol: TYPE_TIMESTAMP = 7,support bind below:
     * <ul>
     *     <li>{@link LocalDateTime}</li>
     *     <li>{@link java.time.OffsetDateTime},As of MySQL 8.0.19.</li>
     *     <li>{@link java.time.ZonedDateTime},As of MySQL 8.0.19.</li>
     * </ul>
     * Only support get {@link LocalDateTime}.
     * </p>
     */
    // TODO If MySQL server run with the MAXDB SQL mode enabled, TIMESTAMP is identical with DATETIME. If this mode is enabled at the time that a table is created, TIMESTAMP columns are created as DATETIME columns.
    // As a result, such columns use DATETIME display format, have the same range of values, and there is no automatic initialization or updating to the current date and time
    TIMESTAMP(Constants.TYPE_TIMESTAMP, JdbdType.TIMESTAMP, LocalDateTime.class),

    /**
     * [NATIONAL] CHAR[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]
     * A fixed-length string that is always right-padded with spaces to the specified length when stored.
     * M represents the column length in characters. The range of M is 0 to 255. If M is omitted, the length is 1.
     * Note
     * Trailing spaces are removed when CHAR values are retrieved unless the PAD_CHAR_TO_FULL_LENGTH SQL mode is enabled.
     * CHAR is shorthand for CHARACTER. NATIONAL CHAR (or its equivalent short form, NCHAR) is the standard SQL way
     * to define that a CHAR column should use some predefined character set. MySQL 4.1 and up uses utf8
     * as this predefined character set.
     * <p>
     * MySQL permits you to create a column of type CHAR(0). This is useful primarily when you have to be compliant
     * with old applications that depend on the existence of a column but that do not actually use its value.
     * CHAR(0) is also quite nice when you need a column that can take only two values: A column that is defined
     * as CHAR(0) NULL occupies only one bit and can take only the values NULL and '' (the empty string).
     * <p>
     * Protocol: TYPE_STRING = 254
     */
    CHAR(Constants.TYPE_STRING, JdbdType.CHAR, String.class),

    /**
     * [NATIONAL] VARCHAR(M) [CHARACTER SET charset_name] [COLLATE collation_name]
     * A variable-length string. M represents the maximum column length in characters. The range of M is 0 to 65,535.
     * The effective maximum length of a VARCHAR is subject to the maximum row size (65,535 bytes, which is shared among
     * all columns) and the character set used. For example, utf8 characters can require up to three bytes per character,
     * so a VARCHAR column that uses the utf8 character set can be declared to be a maximum of 21,844 characters.
     * <p>
     * MySQL stores VARCHAR values as a 1-byte or 2-byte length prefix plus data. The length prefix indicates the number
     * of bytes in the value. A VARCHAR column uses one length byte if values require no more than 255 bytes, two length
     * bytes if values may require more than 255 bytes.
     * <p>
     * Note
     * MySQL 5.7 follows the standard SQL specification, and does not remove trailing spaces from VARCHAR values.
     * <p>
     * VARCHAR is shorthand for CHARACTER VARYING. NATIONAL VARCHAR is the standard SQL way to define that a VARCHAR
     * column should use some predefined character set. MySQL 4.1 and up uses utf8 as this predefined character set.
     * NVARCHAR is shorthand for NATIONAL VARCHAR.
     * <p>
     * Protocol: TYPE_VARCHAR = 15
     * Protocol: TYPE_VAR_STRING = 253
     */
    VARCHAR(Constants.TYPE_VARCHAR, JdbdType.VARCHAR, String.class),

    /**
     * BIT[(M)]
     * A bit-field type. M indicates the number of bits per value, from 1 to 64. The default is 1 if M is omitted.
     * Protocol: TYPE_BIT = 16
     */
    BIT(Constants.TYPE_BIT, JdbdType.BIT, Long.class),

    /**
     * ENUM('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]
     * An enumeration. A string object that can have only one value, chosen from the list of values 'value1',
     * 'value2', ..., NULL or the special '' error value. ENUM values are represented internally as integers.
     * An ENUM column can have a maximum of 65,535 distinct elements. (The practical limit is less than 3000.)
     * A table can have no more than 255 unique element list definitions among its ENUM and SET columns considered as a group
     * <p>
     * Protocol: TYPE_ENUM = 247
     */
    ENUM(Constants.TYPE_ENUM, JdbdType.VARCHAR, String.class),
    /**
     * SET('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]
     * A set. A string object that can have zero or more values, each of which must be chosen from the list
     * of values 'value1', 'value2', ... SET values are represented internally as integers.
     * A SET column can have a maximum of 64 distinct members. A table can have no more than 255 unique
     * element list definitions among its ENUM and SET columns considered as a group
     * <p>
     * Protocol: TYPE_SET = 248
     * <p>
     * a unmodifiable {@link Set} ,the type of elements is {@link String} or {@link Enum}
     * </p>
     */
    SET(Constants.TYPE_SET, JdbdType.VARCHAR, String.class),

    /**
     * The size of JSON documents stored in JSON columns is limited to the value of the max_allowed_packet system variable (max value 1073741824).
     * (While the server manipulates a JSON value internally in memory, it can be larger; the limit applies when the server stores it.)
     * <p>
     * Protocol: TYPE_BIT = 245
     */
    JSON(Constants.TYPE_JSON, JdbdType.JSON, String.class),

    /**
     * TINYTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 255 (28 - 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each TINYTEXT value is stored using
     * a 1-byte length prefix that indicates the number of bytes in the value.
     * <p>
     * Protocol:TYPE_TINY_BLOB = 249
     */
    TINYTEXT(Constants.TYPE_TINY_BLOB, JdbdType.TINYTEXT, String.class),

    /**
     * MEDIUMTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 16,777,215 (224 - 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each MEDIUMTEXT value is stored using a 3-byte
     * length prefix that indicates the number of bytes in the value.
     * <p>
     * Protocol: TYPE_MEDIUM_BLOB = 250
     */
    MEDIUMTEXT(Constants.TYPE_MEDIUM_BLOB, JdbdType.MEDIUMTEXT, String.class),

    /**
     * TEXT[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 65,535 (216 - 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each TEXT value is stored using a 2-byte length
     * prefix that indicates the number of bytes in the value.
     * An optional length M can be given for this type. If this is done, MySQL creates the column as
     * the smallest TEXT type large enough to hold values M characters long.
     * <p>
     * Protocol: TYPE_BLOB = 252
     */
    TEXT(Constants.TYPE_BLOB, JdbdType.TEXT, String.class),


    /**
     * LONGTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 4,294,967,295 or 4GB (232 - 1) characters. The effective
     * maximum length is less if the value contains multibyte characters. The effective maximum length
     * of LONGTEXT columns also depends on the configured maximum packet size in the client/server protocol
     * and available memory. Each LONGTEXT value is stored using a 4-byte length prefix that indicates
     * the number of bytes in the value.
     * <p>
     * Protocol: TYPE_LONG_BLOB = 251
     */
    LONGTEXT(Constants.TYPE_LONG_BLOB, JdbdType.LONGTEXT, String.class),

    /**
     * BINARY(M)
     * The BINARY type is similar to the CHAR type, but stores binary byte strings rather than nonbinary character strings.
     * M represents the column length in bytes.
     * <p>
     * The CHAR BYTE data type is an alias for the BINARY data type.
     * <p>
     * Protocol: no concrete type on the wire
     */
    BINARY(Constants.TYPE_STRING, JdbdType.BINARY, byte[].class),

    /**
     * VARBINARY(M)
     * The VARBINARY type is similar to the VARCHAR type, but stores binary byte strings rather than nonbinary
     * character strings. M represents the maximum column length in bytes.
     * <p>
     * Protocol: TYPE_VARCHAR = 15
     * Protocol: TYPE_VAR_STRING = 253
     */
    VARBINARY(Constants.TYPE_VAR_STRING, JdbdType.VARBINARY, byte[].class),

    /**
     * TINYBLOB
     * A BLOB column with a maximum length of 255 (28 - 1) bytes. Each TINYBLOB value is stored using a
     * 1-byte length prefix that indicates the number of bytes in the value.
     * <p>
     * Protocol:TYPE_TINY_BLOB = 249
     */
    TINYBLOB(Constants.TYPE_TINY_BLOB, JdbdType.TINYBLOB, byte[].class),

    /**
     * MEDIUMBLOB
     * A BLOB column with a maximum length of 16,777,215 (224 - 1) bytes. Each MEDIUMBLOB value is stored
     * using a 3-byte length prefix that indicates the number of bytes in the value.
     * <p>
     * Protocol: TYPE_MEDIUM_BLOB = 250
     */
    MEDIUMBLOB(Constants.TYPE_MEDIUM_BLOB, JdbdType.MEDIUMBLOB, byte[].class),

    /**
     * BLOB[(M)]
     * A BLOB column with a maximum length of 65,535 (216 - 1) bytes. Each BLOB value is stored using
     * a 2-byte length prefix that indicates the number of bytes in the value.
     * An optional length M can be given for this type. If this is done, MySQL creates the column as
     * the smallest BLOB type large enough to hold values M bytes long.
     * <p>
     * Protocol: TYPE_BLOB = 252
     */
    BLOB(Constants.TYPE_BLOB, JdbdType.BLOB, byte[].class),


    /**
     * LONGBLOB
     * A BLOB column with a maximum length of 4,294,967,295 or 4GB (232 - 1) bytes. The effective maximum length
     * of LONGBLOB columns depends on the configured maximum packet size in the client/server protocol and available
     * memory. Each LONGBLOB value is stored using a 4-byte length prefix that indicates the number of bytes in the value.
     * <p>
     * Protocol: TYPE_LONG_BLOB = 251
     */
    LONGBLOB(Constants.TYPE_LONG_BLOB, JdbdType.LONGBLOB, byte[].class),

    /**
     * Top class for Spatial Data Types,that is WKB.
     * <p>
     * Protocol: TYPE_GEOMETRY = 255
     */
    GEOMETRY(Constants.TYPE_GEOMETRY, JdbdType.GEOMETRY, byte[].class),

    /**
     * TYPE_NULL = 6
     */
    NULL(Constants.TYPE_NULL, JdbdType.NULL, Object.class),

    /**
     * Fall-back type for those MySQL data types which c/J can't recognize.
     * Handled the same as BLOB.
     * <p>
     * Has no protocol ID.
     */
    UNKNOWN((short) -1, JdbdType.UNKNOWN, Object.class);


    private final JdbdType jdbdType;

    private final Class<?> javaType;

    public final short typeFlag;

    public final boolean unsigned;

    public final short parameterType;

    MySQLType(short typeFlag, JdbdType jdbcType, Class<?> javaType) {
        this(typeFlag, false, jdbcType, javaType);
    }

    MySQLType(short typeFlag, boolean unsigned, JdbdType jdbdType, Class<?> javaType) {
        this.typeFlag = typeFlag;
        this.unsigned = unsigned;
        this.jdbdType = jdbdType;
        this.javaType = javaType;

        this.parameterType = (short) (unsigned ? (0x8000 | typeFlag) : typeFlag);
    }


    @Override
    public final JdbdType jdbdType() {
        return this.jdbdType;
    }

    @Override
    public final String typeName() {
        final String sqlTypeName;
        switch (this) {
            case TINYINT_UNSIGNED:
                sqlTypeName = "TINYINT UNSIGNED";
                break;
            case SMALLINT_UNSIGNED:
                sqlTypeName = "SMALLINT UNSIGNED";
                break;
            case MEDIUMINT_UNSIGNED:
                sqlTypeName = "MEDIUMINT UNSIGNED";
                break;
            case INT_UNSIGNED:
                sqlTypeName = "INT UNSIGNED";
                break;
            case BIGINT_UNSIGNED:
                sqlTypeName = "BIGINT UNSIGNED";
                break;
            case DOUBLE_UNSIGNED:
                sqlTypeName = "DOUBLE UNSIGNED";
                break;
            case FLOAT_UNSIGNED:
                sqlTypeName = "FLOAT UNSIGNED";
                break;
            case DECIMAL_UNSIGNED:
                sqlTypeName = "DECIMAL UNSIGNED";
                break;
            default:
                sqlTypeName = this.name();
        }
        return sqlTypeName;
    }

    @Override
    public final Class<?> firstJavaType() {
        return this.javaType;
    }

    @Override
    public final Class<?> secondJavaType() {
        final Class<?> type;
        switch (this) {
            case TIME:
                type = Duration.class;
                break;
            case LONGTEXT:
            case JSON:
                type = TextPath.class;
                break;
            case GEOMETRY:
            case LONGBLOB:
                type = BlobPath.class;
                break;
            default:
                type = null;
        }
        return type;
    }


    @Override
    public final SQLType elementType() {
        //always null,MySQL don't support array
        return null;
    }

    @Override
    public final String vendor() {
        return MySQLDriver.DRIVER_VENDOR;
    }

    @Override
    public final boolean isArray() {
        //always false,MySQL don't support array
        return false;
    }

    @Override
    public final boolean isUnknown() {
        return this == UNKNOWN;
    }

    @Override
    public final boolean isUserDefined() {
        //MySQL don't support user defined type
        return false;
    }


}
