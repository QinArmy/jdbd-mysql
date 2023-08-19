package io.jdbd.mysql.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.meta.NullMode;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.FieldType;
import io.jdbd.vendor.result.ColumnMeta;
import io.netty.buffer.ByteBuf;
import io.qinarmy.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html"> Column Definition Flags</a>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html"> Column Definition Protocol</a>
 */
final class MySQLColumnMeta implements ColumnMeta {

    static final MySQLColumnMeta[] EMPTY = new MySQLColumnMeta[0];

    private static final Logger LOG = LoggerFactory.getLogger(MySQLColumnMeta.class);

    static final int NOT_NULL_FLAG = 1;
    static final int PRI_KEY_FLAG = 1 << 1;
    static final int UNIQUE_KEY_FLAG = 1 << 2;
    static final int MULTIPLE_KEY_FLAG = 1 << 3;

    static final int BLOB_FLAG = 1 << 4;
    static final int UNSIGNED_FLAG = 1 << 5;
    static final int ZEROFILL_FLAG = 1 << 6;
    static final int BINARY_FLAG = 1 << 7;

    static final int ENUM_FLAG = 1 << 8;
    static final int AUTO_INCREMENT_FLAG = 1 << 9;
    static final int TIMESTAMP_FLAG = 1 << 10;
    static final int SET_FLAG = 1 << 11;

    static final int NO_DEFAULT_VALUE_FLAG = 1 << 12;
    static final int ON_UPDATE_NOW_FLAG = 1 << 13;
    static final int PART_KEY_FLAG = 1 << 14;
    static final int NUM_FLAG = 1 << 15;

    final int columnIndex;

    final String catalogName;

    final String schemaName;

    final String tableName;

    final String tableLabel;

    final String columnName;

    final String columnLabel;

    final int collationIndex;

    final Charset columnCharset;

    final long fixedLength;

    final long length;

    final int typeFlag;

    final int definitionFlags;

    final short decimals;

    final FieldType fieldType;

    final MySQLType sqlType;


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html">Protocol::ColumnDefinition41</a>
     */
    private MySQLColumnMeta(int columnIndex, final ByteBuf cumulateBuffer, final Charset metaCharset,
                            final Map<Integer, Charsets.CustomCollation> customCollationMap, final FixedEnv env) {

        this.columnIndex = columnIndex;
        // 1. catalog
        this.catalogName = Packets.readStringLenEnc(cumulateBuffer, metaCharset);
        // 2. schema
        this.schemaName = Packets.readStringLenEnc(cumulateBuffer, metaCharset);
        // 3. table,virtual table name
        this.tableLabel = Packets.readStringLenEnc(cumulateBuffer, metaCharset);
        // 4. org_table,physical table name
        this.tableName = Packets.readStringLenEnc(cumulateBuffer, metaCharset);

        // 5. name ,virtual column name,alias in select statement
        final String columnLabel;
        columnLabel = Packets.readStringLenEnc(cumulateBuffer, metaCharset);
        if (columnLabel == null) {
            throw new NullPointerException("columnLabel is null, MySQL protocol error");
        }
        this.columnLabel = columnLabel;
        // 6. org_name,physical column name
        this.columnName = Packets.readStringLenEnc(cumulateBuffer, metaCharset);
        // 7. length of fixed length fields ,[0x0c]
        //
        this.fixedLength = Packets.readLenEnc(cumulateBuffer);
        // 8. character_set of column
        this.collationIndex = Packets.readInt2AsInt(cumulateBuffer);
        this.columnCharset = Charsets.getJavaCharsetByCollationIndex(this.collationIndex, customCollationMap);
        // 9. column_length,maximum length of the field
        this.length = Packets.readInt4AsLong(cumulateBuffer);
        // 10. type,type of the column as defined in enum_field_types,type of the column as defined in enum_field_types
        this.typeFlag = Packets.readInt1AsInt(cumulateBuffer);
        // 11. flags,Flags as defined in Column Definition Flags
        this.definitionFlags = Packets.readInt2AsInt(cumulateBuffer);
        // 12. decimals,max shown decimal digits:
        //0x00 for integers and static strings
        //0x1f for dynamic strings, double, float
        //0x00 to 0x51 for decimals
        this.decimals = (short) Packets.readInt1AsInt(cumulateBuffer);

        this.fieldType = parseFieldType(this);
        this.sqlType = parseSqlType(this, env);
    }

    @Override
    public int getColumnIndex() {
        return this.columnIndex;
    }

    @Override
    public boolean isBit() {
        return false;
    }

    @Override
    public boolean isUnsigned() {
        return (this.definitionFlags & UNSIGNED_FLAG) != 0;
    }


    public long obtainPrecision(Map<Integer, Charsets.CustomCollation> customCollationMap) {
        long precision;
        // Protocol returns precision and scale differently for some types. We need to align then to I_S.
        switch (this.sqlType) {
            case DECIMAL:
                precision = this.length;
                precision--;// signed
                if (this.decimals > 0) {
                    precision--; // point
                }
                break;
            case DECIMAL_UNSIGNED:
                precision = this.length;
                if (this.decimals > 0) {
                    precision--;// point
                }
                break;
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case BIT:
                precision = this.length;
                break;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                // char
                int collationIndex = this.collationIndex;
                Charsets.CustomCollation collation = customCollationMap.get(collationIndex);
                Integer mblen = null;
                if (collation != null) {
                    mblen = collation.maxLen;
                }
                if (mblen == null) {
                    mblen = Charsets.getMblen(collationIndex);
                }
                precision = this.length / mblen;
                break;
            case TIME:
                precision = obtainTimeTypePrecision();
                break;
            case TIMESTAMP:
            case DATETIME:
                precision = getDateTimeTypePrecision();
                break;
            default:
                precision = -1;

        }
        return precision;
    }


    @Override
    public DataType getDataType() {
        return this.sqlType;
    }

    @Override
    public String getColumnLabel() {
        return this.columnLabel;
    }

    public int getScale() {
        final int scale;
        switch (sqlType) {
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                scale = this.decimals;
                break;
            default:
                scale = -1;
        }
        return scale;
    }


    final boolean isEnum() {
        return (this.definitionFlags & ENUM_FLAG) != 0;
    }

    final boolean isSetType() {
        return (this.definitionFlags & SET_FLAG) != 0;
    }

    boolean isBinary() {
        return (this.definitionFlags & BINARY_FLAG) != 0;
    }

    final boolean isBlob() {
        return (this.definitionFlags & BLOB_FLAG) != 0;
    }

    final boolean isAutoIncrement() {
        return (this.definitionFlags & AUTO_INCREMENT_FLAG) != 0;
    }

    final boolean isPrimaryKey() {
        return (this.definitionFlags & PRI_KEY_FLAG) != 0;
    }

    final boolean isMultipleKey() {
        return (this.definitionFlags & MULTIPLE_KEY_FLAG) != 0;
    }

    final boolean isUniqueKey() {
        return (this.definitionFlags & UNIQUE_KEY_FLAG) != 0;
    }


    final NullMode getNullMode() {
        final NullMode nullMode;
        if ((this.definitionFlags & NOT_NULL_FLAG) != 0 || (this.definitionFlags & PRI_KEY_FLAG) != 0) {
            nullMode = NullMode.NON_NULL;
        } else {
            nullMode = NullMode.NULLABLE;
        }
        return nullMode;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MySQLColumnMeta{");
        sb.append("\ncatalogName='").append(catalogName).append('\'')
                .append(",\n schemaName='").append(schemaName).append('\'')
                .append(",\n tableName='").append(tableName).append('\'')
                .append(",\n tableAlias='").append(tableLabel).append('\'')
                .append(",\n columnName='").append(columnName).append('\'')
                .append(",\n columnAlias='").append(columnLabel).append('\'')
                .append(",\n collationIndex=").append(collationIndex)
                .append(",\n fixedLength=").append(fixedLength)
                .append(",\n length=").append(length)
                .append(",\n typeFlag=").append(typeFlag)
                .append(",\n definitionFlags={\n");

        appendDefinitionFlag(sb);

        sb.append(",decimals=").append(decimals)
                .append(",\n mysqlType=").append(sqlType)
                .append('}');
        return sb.toString();
    }

    private void appendDefinitionFlag(final StringBuilder builder) {
        final int flag = this.definitionFlags;
        final char[] bitCharMap = new char[16];
        Arrays.fill(bitCharMap, '.');
        int index = bitCharMap.length - 1;

        bitCharMap[index] = (flag & NOT_NULL_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Not null\n");

        bitCharMap[index] = (flag & PRI_KEY_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Primary key\n");

        bitCharMap[index] = (flag & UNIQUE_KEY_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Unique key\n");

        bitCharMap[index] = (flag & MULTIPLE_KEY_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Multiple key\n");


        bitCharMap[index] = (flag & BLOB_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Blob\n");

        bitCharMap[index] = (flag & UNSIGNED_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Unsigned\n");

        bitCharMap[index] = (flag & ZEROFILL_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Zoro fill\n");

        bitCharMap[index] = (flag & BINARY_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Binary\n");


        bitCharMap[index] = (flag & ENUM_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Enum\n");

        bitCharMap[index] = (flag & AUTO_INCREMENT_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Auto increment\n");

        bitCharMap[index] = (flag & TIMESTAMP_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Timestamp\n");

        bitCharMap[index] = (flag & SET_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Set\n");


        bitCharMap[index] = (flag & NO_DEFAULT_VALUE_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = No default\n");

        bitCharMap[index] = (flag & ON_UPDATE_NOW_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = On update now\n");

        bitCharMap[index] = (flag & PART_KEY_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index--] = '.';
        builder.append(" = Part key\n");

        bitCharMap[index] = (flag & NUM_FLAG) == 0 ? '0' : '1';
        builder.append(bitCharMap);
        bitCharMap[index] = '.';
        builder.append(" = Num\n}");

    }

    int obtainTimeTypePrecision() {
        final int precision;
        if (this.decimals > 0 && this.decimals < 7) {
            precision = this.decimals;
        } else if (this.length == 10) {
            precision = 0;
        } else {
            precision = (int) (this.length - 11L);
            if (precision < 0 || precision > 6) {
                throw new IllegalArgumentException(String.format("MySQLColumnMeta[%s] isn't time type.", this));
            }
        }
        return precision;
    }


    int getDateTimeTypePrecision() {
        final int precision;
        if (this.decimals > 0 && this.decimals < 7) {
            precision = this.decimals;
        } else if (this.length == 19) {
            precision = 0;
        } else {
            precision = (int) (this.length - 20L);
            if (precision < 0 || precision > 6) {
                throw new IllegalArgumentException(String.format("MySQLColumnMeta[%s] isn't time type.", this));
            }
        }
        return precision;
    }


    static MySQLColumnMeta[] readMetas(final ByteBuf cumulateBuffer, final int columnCount, final MetaAdjutant metaAdjutant) {

        final MySQLColumnMeta[] metaArray = columnCount == 0 ? EMPTY : new MySQLColumnMeta[columnCount];

        final TaskAdjutant adjutant = metaAdjutant.adjutant();
        final Charset metaCharset = adjutant.obtainCharsetMeta();
        final FixedEnv env = adjutant.getFactory();
        final Map<Integer, Charsets.CustomCollation> customCollationMap = adjutant.obtainCustomCollationMap();

        final boolean debug = LOG.isDebugEnabled();
        int sequenceId = -1;
        for (int i = 0, payloadLength, payloadIndex; i < columnCount; i++) {
            payloadLength = Packets.readInt3(cumulateBuffer);
            sequenceId = Packets.readInt1AsInt(cumulateBuffer);

            payloadIndex = cumulateBuffer.readerIndex();

            metaArray[i] = new MySQLColumnMeta(i, cumulateBuffer, metaCharset, customCollationMap, env);
            if (debug) {
                LOG.debug("column[{}] {}", i, metaArray[i]);
            }
            cumulateBuffer.readerIndex(payloadIndex + payloadLength); //avoid tail filler
        }
        if (sequenceId > -1) {
            metaAdjutant.updateSequenceId(sequenceId);
        }
        return metaArray;
    }


    private static FieldType parseFieldType(final MySQLColumnMeta columnMeta) {
        final String tableName = columnMeta.tableName;
        final FieldType fieldType;
        if (!MySQLStrings.hasText(tableName)) {
            fieldType = FieldType.EXPRESSION;
        } else if (tableName.startsWith("#sql_")) {// TODO zoro complete
            fieldType = FieldType.PHYSICAL_FILED;
        } else {
            fieldType = FieldType.FIELD;
        }
        return fieldType;
    }


    @SuppressWarnings("deprecation")
    private static MySQLType parseSqlType(final MySQLColumnMeta columnMeta, final FixedEnv env) {
        final MySQLType type;
        switch (columnMeta.typeFlag) {
            case Constants.TYPE_DECIMAL:
            case Constants.TYPE_NEWDECIMAL:
                type = columnMeta.isUnsigned() ? MySQLType.DECIMAL_UNSIGNED : MySQLType.DECIMAL;
                break;
            case Constants.TYPE_TINY: {
                final boolean unsigned = columnMeta.isUnsigned();
                if (columnMeta.length == 1 && unsigned && env.transformedBitIsBoolean) {
                    type = MySQLType.BOOLEAN;
                } else {
                    type = unsigned ? MySQLType.TINYINT_UNSIGNED : MySQLType.TINYINT;
                }
            }
            break;
            case Constants.TYPE_LONG:
                type = columnMeta.isUnsigned() ? MySQLType.INT_UNSIGNED : MySQLType.INT;
                break;
            case Constants.TYPE_LONGLONG:
                type = columnMeta.isUnsigned() ? MySQLType.BIGINT_UNSIGNED : MySQLType.BIGINT;
                break;
            case Constants.TYPE_TIMESTAMP:
                type = MySQLType.TIMESTAMP;
                break;
            case Constants.TYPE_INT24:
                type = columnMeta.isUnsigned() ? MySQLType.MEDIUMINT_UNSIGNED : MySQLType.MEDIUMINT;
                break;
            case Constants.TYPE_DATE:
                type = MySQLType.DATE;
                break;
            case Constants.TYPE_TIME:
                type = MySQLType.TIME;
                break;
            case Constants.TYPE_DATETIME:
                type = MySQLType.DATETIME;
                break;
            case Constants.TYPE_YEAR:
                type = MySQLType.YEAR;
                break;
            case Constants.TYPE_VARCHAR:
            case Constants.TYPE_VAR_STRING:
                type = fromVarcharOrVarString(columnMeta, env);
                break;
            case Constants.TYPE_STRING:
                type = fromString(columnMeta);
                break;
            case Constants.TYPE_SHORT:
                type = columnMeta.isUnsigned() ? MySQLType.SMALLINT_UNSIGNED : MySQLType.SMALLINT;
                break;
            case Constants.TYPE_BIT:
                type = MySQLType.BIT;
                break;
            case Constants.TYPE_JSON:
                type = MySQLType.JSON;
                break;
            case Constants.TYPE_ENUM:
                type = MySQLType.ENUM;
                break;
            case Constants.TYPE_SET:
                type = MySQLType.SET;
                break;
            case Constants.TYPE_NULL:
                type = MySQLType.NULL;
                break;
            case Constants.TYPE_FLOAT:
                type = columnMeta.isUnsigned() ? MySQLType.FLOAT_UNSIGNED : MySQLType.FLOAT;
                break;
            case Constants.TYPE_DOUBLE:
                type = columnMeta.isUnsigned() ? MySQLType.DOUBLE_UNSIGNED : MySQLType.DOUBLE;
                break;
            case Constants.TYPE_TINY_BLOB: {
                type = fromTinyBlob(columnMeta, env);
            }
            break;
            case Constants.TYPE_MEDIUM_BLOB: {
                type = fromMediumBlob(columnMeta, env);
            }
            break;
            case Constants.TYPE_LONG_BLOB: {
                type = fromLongBlob(columnMeta, env);
            }
            break;
            case Constants.TYPE_BLOB:
                type = fromBlob(columnMeta, env);
                break;
            case Constants.TYPE_BOOL:
                type = MySQLType.BOOLEAN;
                break;
            case Constants.TYPE_GEOMETRY:
                type = MySQLType.GEOMETRY;
                break;
            default:
                type = MySQLType.UNKNOWN;
        }
        return type;
    }


    private static MySQLType fromVarcharOrVarString(final MySQLColumnMeta meta, final FixedEnv env) {
        final MySQLType type;
        if (meta.isEnum()) {
            type = MySQLType.ENUM;
        } else if (meta.isSetType()) {
            type = MySQLType.SET;
        } else if (isOpaqueBinary(meta) && !isFunctionsNeverReturnBlobs(meta, env)) {
            type = MySQLType.VARBINARY;
        } else {
            type = MySQLType.VARCHAR;
        }
        return type;
    }

    private static MySQLType fromBlob(final MySQLColumnMeta columnMeta, final FixedEnv env) {
        // Sometimes MySQL uses this protocol-level type for all possible BLOB variants,
        // we can divine what the actual type is by the length reported

        final MySQLType mySQLType;

        final long maxLength = columnMeta.length;
        // fixing initial type according to length
        if (maxLength <= 255L) {
            mySQLType = fromTinyBlob(columnMeta, env);
        } else if (columnMeta.length <= (1 << 16) - 1) {
            if (columnMeta.isBinary() || !isBlobTypeReturnText(columnMeta, env)) {
                mySQLType = MySQLType.BLOB;
            } else {
                mySQLType = MySQLType.TEXT;
            }
        } else if (maxLength <= (1 << 24) - 1) {
            mySQLType = fromMediumBlob(columnMeta, env);
        } else {
            mySQLType = fromLongBlob(columnMeta, env);
        }
        return mySQLType;
    }

    private static MySQLType fromTinyBlob(final MySQLColumnMeta columnMeta, final FixedEnv env) {
        final MySQLType mySQLType;
        if (columnMeta.isBinary() || !isBlobTypeReturnText(columnMeta, env)) {
            mySQLType = MySQLType.TINYBLOB;
        } else {
            mySQLType = MySQLType.TINYTEXT;
        }
        return mySQLType;
    }

    private static MySQLType fromMediumBlob(final MySQLColumnMeta columnMeta, final FixedEnv env) {
        final MySQLType mySQLType;
        if (columnMeta.isBinary() || !isBlobTypeReturnText(columnMeta, env)) {
            mySQLType = MySQLType.MEDIUMBLOB;
        } else {
            mySQLType = MySQLType.MEDIUMTEXT;
        }
        return mySQLType;
    }

    private static MySQLType fromLongBlob(final MySQLColumnMeta columnMeta, final FixedEnv env) {
        final MySQLType mySQLType;
        if (columnMeta.isBinary() || !isBlobTypeReturnText(columnMeta, env)) {
            mySQLType = MySQLType.LONGBLOB;
        } else {
            mySQLType = MySQLType.LONGTEXT;
        }
        return mySQLType;
    }

    private static MySQLType fromString(MySQLColumnMeta columnMeta) {
        final MySQLType mySQLType;
        if (columnMeta.isEnum()) {
            mySQLType = MySQLType.ENUM;
        } else if (columnMeta.isSetType()) {
            mySQLType = MySQLType.SET;
        } else if (columnMeta.isBinary() || isOpaqueBinary(columnMeta)) {
            mySQLType = MySQLType.BINARY;
        } else {
            mySQLType = MySQLType.CHAR;
        }
        return mySQLType;
    }

    private static boolean isOpaqueBinary(MySQLColumnMeta columnMeta) {

        boolean isImplicitTemporaryTable = columnMeta.tableName != null
                && columnMeta.tableName.startsWith("#sql_"); //TODO check tableName or tableAlias

        boolean isBinaryString = columnMeta.isBinary()
                && columnMeta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary
                && (columnMeta.typeFlag == Constants.TYPE_STRING
                || columnMeta.typeFlag == Constants.TYPE_VAR_STRING
                || columnMeta.typeFlag == Constants.TYPE_VARCHAR);

        return isBinaryString
                // queries resolved by temp tables also have this 'signature', check for that
                ? !isImplicitTemporaryTable : columnMeta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary;

    }

    private static boolean isFunctionsNeverReturnBlobs(final MySQLColumnMeta meta, final FixedEnv env) {
        return StringUtils.isEmpty(meta.tableName) && env.functionsNeverReturnBlobs;
    }

    private static boolean isBlobTypeReturnText(final MySQLColumnMeta meta, final FixedEnv env) {
        return !meta.isBinary()
                || meta.collationIndex != Charsets.MYSQL_COLLATION_INDEX_binary
                || env.blobsAreStrings
                || isFunctionsNeverReturnBlobs(meta, env);
    }


}
