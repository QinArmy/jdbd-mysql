/*
 * Copyright 2023-2043 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jdbd.mysql.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.vendor.result.ColumnMeta;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html"> Column Definition Flags</a>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html"> Column Definition Protocol</a>
 */
final class MySQLColumnMeta implements ColumnMeta {

    static final MySQLColumnMeta[] EMPTY = new MySQLColumnMeta[0];

    private static final Logger LOG = LoggerFactory.getLogger(MySQLColumnMeta.class);

    static final byte NOT_NULL_FLAG = 1;
    static final byte PRI_KEY_FLAG = 1 << 1;
    static final byte UNIQUE_KEY_FLAG = 1 << 2;
    static final byte MULTIPLE_KEY_FLAG = 1 << 3;

    static final byte BLOB_FLAG = 1 << 4;
    static final byte UNSIGNED_FLAG = 1 << 5;
    static final byte ZEROFILL_FLAG = 1 << 6;
    static final short BINARY_FLAG = 1 << 7;

    static final short ENUM_FLAG = 1 << 8;
    static final short AUTO_INCREMENT_FLAG = 1 << 9;
    static final short TIMESTAMP_FLAG = 1 << 10;
    static final short SET_FLAG = 1 << 11;

    static final short NO_DEFAULT_VALUE_FLAG = 1 << 12;
    static final short ON_UPDATE_NOW_FLAG = 1 << 13;
    static final short PART_KEY_FLAG = 1 << 14;
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

    final MySQLType sqlType;


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html">Protocol::ColumnDefinition41</a>
     */
    private MySQLColumnMeta(int columnIndex, final ByteBuf cumulateBuffer, final Charset metaCharset,
                            final Map<Integer, CustomCollation> customCollationMap) {

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
        this.columnCharset = Charsets.tryGetJavaCharsetByCollationIndex(this.collationIndex, customCollationMap);
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

        this.sqlType = parseSqlType(this);
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

    long obtainPrecision(final Map<Integer, CustomCollation> customCollationMap) {
        long precision;
        // Protocol returns precision and scale differently for some types. We need to align then to I_S.
        switch (this.sqlType) {
            case DECIMAL: {
                precision = this.length;
                precision--;// signed
                if (this.decimals > 0) {
                    precision--; // point
                }
            }
            break;
            case DECIMAL_UNSIGNED: {
                precision = this.length;
                if (this.decimals > 0) {
                    precision--;// point
                }
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
            case LONGTEXT: {
                // char
                int collationIndex = this.collationIndex;
                CustomCollation collation = customCollationMap.get(collationIndex);
                Integer mblen = null;
                if (collation != null) {
                    mblen = collation.maxLen;
                }
                if (mblen == null) {
                    mblen = Charsets.getMblen(collationIndex);
                }
                precision = this.length / mblen;
            }
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

    int getScale() {
        final int scale;
        switch (sqlType) {
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                scale = this.decimals;
                break;
            case TIME:
                scale = obtainTimeTypePrecision();
                break;
            case TIMESTAMP:
            case DATETIME:
                scale = getDateTimeTypePrecision();
                break;
            default:
                scale = -1;
        }
        return scale;
    }


    boolean isEnum() {
        return (this.definitionFlags & ENUM_FLAG) != 0;
    }

    boolean isSetType() {
        return (this.definitionFlags & SET_FLAG) != 0;
    }

    boolean isBinary() {
        return (this.definitionFlags & BINARY_FLAG) != 0;
    }


    boolean isAutoIncrement() {
        return (this.definitionFlags & AUTO_INCREMENT_FLAG) != 0;
    }


    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder(725);
        builder.append(getClass().getSimpleName())
                .append("{\ncatalogName='")
                .append(catalogName)
                .append('\'')
                .append(",\n schemaName='")
                .append(schemaName)
                .append('\'')
                .append(",\n tableName='")
                .append(tableName)
                .append('\'')
                .append(",\n tableAlias='")
                .append(tableLabel)
                .append('\'')
                .append(",\n columnName='")
                .append(columnName)
                .append('\'')
                .append(",\n columnAlias='")
                .append(columnLabel)
                .append('\'')
                .append(",\n collationIndex=")
                .append(collationIndex)
                .append(",\n fixedLength=")
                .append(fixedLength)
                .append(",\n length=")
                .append(length)
                .append(",\n typeFlag=")
                .append(typeFlag)
                .append(",\n definitionFlags={\n");

        appendDefinitionFlag(builder);

        return builder.append(",decimals=")
                .append(decimals)
                .append(",\n mysqlType=")
                .append(sqlType)
                .append('}')
                .toString();
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

    private int obtainTimeTypePrecision() {
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


    private int getDateTimeTypePrecision() {
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

    /**
     * @return non-empty : unknown collation index.
     */
    static Set<Integer> readMetas(final ByteBuf cumulateBuffer, final MySQLColumnMeta[] metaArray,
                                  final MetaAdjutant metaAdjutant) {

        final int columnCount = metaArray.length;
        final TaskAdjutant adjutant = metaAdjutant.adjutant();
        final Charset metaCharset = adjutant.obtainCharsetMeta();

        final Map<Integer, CustomCollation> customCollationMap = adjutant.obtainCustomCollationMap();

        final boolean debug = LOG.isDebugEnabled();
        int sequenceId = -1;
        MySQLColumnMeta columnMeta;
        Set<Integer> unknownCollationSet = null;
        for (int i = 0, payloadLength, payloadIndex; i < columnCount; i++) {
            payloadLength = Packets.readInt3(cumulateBuffer);
            sequenceId = Packets.readInt1AsInt(cumulateBuffer);

            payloadIndex = cumulateBuffer.readerIndex();

            columnMeta = new MySQLColumnMeta(i, cumulateBuffer, metaCharset, customCollationMap);
            metaArray[i] = columnMeta;
            if (columnMeta.columnCharset == null) {
                if (unknownCollationSet == null) {
                    unknownCollationSet = MySQLCollections.hashSet();
                }
                unknownCollationSet.add(columnMeta.collationIndex);
            }
            if (debug) {
                LOG.debug("column meta [{}] {}", i, columnMeta);
            }
            cumulateBuffer.readerIndex(payloadIndex + payloadLength); //avoid tail filler
        }
        if (sequenceId > -1) {
            metaAdjutant.updateSequenceId(sequenceId);
        }

        if (unknownCollationSet == null) {
            unknownCollationSet = Collections.emptySet();
        } else {
            unknownCollationSet = MySQLCollections.unmodifiableSet(unknownCollationSet);
        }
        return unknownCollationSet;
    }


    @SuppressWarnings("deprecation")
    private static MySQLType parseSqlType(final MySQLColumnMeta meta) {
        final MySQLType type;
        switch (meta.typeFlag) {
            case Constants.TYPE_DECIMAL:
            case Constants.TYPE_NEWDECIMAL:
                type = meta.isUnsigned() ? MySQLType.DECIMAL_UNSIGNED : MySQLType.DECIMAL;
                break;
            case Constants.TYPE_TINY: {
                final boolean unsigned = meta.isUnsigned();
                if (meta.length == 1 && !unsigned) {
                    type = MySQLType.BOOLEAN;
                } else {
                    type = unsigned ? MySQLType.TINYINT_UNSIGNED : MySQLType.TINYINT;
                }
            }
            break;
            case Constants.TYPE_LONG:
                type = meta.isUnsigned() ? MySQLType.INT_UNSIGNED : MySQLType.INT;
                break;
            case Constants.TYPE_LONGLONG:
                type = meta.isUnsigned() ? MySQLType.BIGINT_UNSIGNED : MySQLType.BIGINT;
                break;
            case Constants.TYPE_TIMESTAMP:
                type = MySQLType.TIMESTAMP;
                break;
            case Constants.TYPE_INT24:
                type = meta.isUnsigned() ? MySQLType.MEDIUMINT_UNSIGNED : MySQLType.MEDIUMINT;
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
            case Constants.TYPE_VAR_STRING: {
                if (meta.isEnum()) {
                    type = MySQLType.ENUM;
                } else if (meta.isSetType()) {
                    type = MySQLType.SET;
                } else if (meta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary) {
                    // https://dev.mysql.com/doc/refman/5.7/en/binary-varbinary.html , VARBINARY have the binary character set and collation
                    type = MySQLType.VARBINARY;
                } else {
                    type = MySQLType.VARCHAR;
                }
            }
            break;
            case Constants.TYPE_STRING: {
                if (meta.isEnum()) {
                    type = MySQLType.ENUM;
                } else if (meta.isSetType()) {
                    type = MySQLType.SET;
                } else if (meta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary) {
                    // https://dev.mysql.com/doc/refman/5.7/en/binary-varbinary.html , VARBINARY have the binary character set and collation
                    type = MySQLType.BINARY;
                } else {
                    type = MySQLType.CHAR;
                }
            }
            break;
            case Constants.TYPE_SHORT:
                type = meta.isUnsigned() ? MySQLType.SMALLINT_UNSIGNED : MySQLType.SMALLINT;
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
                type = meta.isUnsigned() ? MySQLType.FLOAT_UNSIGNED : MySQLType.FLOAT;
                break;
            case Constants.TYPE_DOUBLE:
                type = meta.isUnsigned() ? MySQLType.DOUBLE_UNSIGNED : MySQLType.DOUBLE;
                break;
            case Constants.TYPE_TINY_BLOB: {
                // https://dev.mysql.com/doc/refman/5.7/en/blob.html , blob have binary character set
                if (meta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary) {
                    type = MySQLType.TINYBLOB;
                } else {
                    type = MySQLType.TINYTEXT;
                }
            }
            break;
            case Constants.TYPE_MEDIUM_BLOB: {
                // https://dev.mysql.com/doc/refman/5.7/en/blob.html , blob have binary character set
                if (meta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary) {
                    type = MySQLType.MEDIUMBLOB;
                } else {
                    type = MySQLType.MEDIUMTEXT;
                }
            }
            break;
            case Constants.TYPE_LONG_BLOB: {
                // https://dev.mysql.com/doc/refman/5.7/en/blob.html , blob have binary character set
                if (meta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary) {
                    type = MySQLType.LONGBLOB;
                } else {
                    type = MySQLType.LONGTEXT;
                }
            }
            break;
            case Constants.TYPE_BLOB: {
                final long maxLength = meta.length;
                // https://dev.mysql.com/doc/refman/5.7/en/blob.html , blob have binary character set
                if (maxLength < (1 << 8)) {
                    if (meta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary) {
                        type = MySQLType.TINYBLOB;
                    } else {
                        type = MySQLType.TINYTEXT;
                    }
                } else if (meta.length < (1 << 16)) {
                    if (meta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary) {
                        type = MySQLType.BLOB;
                    } else {
                        type = MySQLType.TEXT;
                    }
                } else if (maxLength < (1 << 24)) {
                    if (meta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary) {
                        type = MySQLType.MEDIUMBLOB;
                    } else {
                        type = MySQLType.MEDIUMTEXT;
                    }
                } else if (meta.collationIndex == Charsets.MYSQL_COLLATION_INDEX_binary) {
                    type = MySQLType.LONGBLOB;
                } else {
                    type = MySQLType.LONGTEXT;
                }
            }
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


}
