package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.*;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.FieldType;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.session.Option;
import io.jdbd.vendor.result.ColumnMeta;
import io.jdbd.vendor.result.VendorResultRowMeta;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;

/**
 * This class is a implementation of {@link ResultRowMeta}
 */
final class MySQLRowMeta extends VendorResultRowMeta {

    /**
     * for {@link #MySQLRowMeta(MySQLColumnMeta[])}
     */
    private static final ZoneOffset PSEUDO_SERVER_ZONE = MySQLTimes.systemZoneOffset();

    static final MySQLRowMeta EMPTY = new MySQLRowMeta(MySQLColumnMeta.EMPTY);


    static boolean canReadMeta(final ByteBuf cumulateBuffer, final boolean eofEnd) {
        final int originalReaderIndex = cumulateBuffer.readerIndex();

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        cumulateBuffer.readByte();// skip sequenceId byte
        final int payloadIndex = cumulateBuffer.readerIndex();
        final int needPacketCount;
        if (eofEnd) {
            needPacketCount = Packets.readLenEncAsInt(cumulateBuffer) + 1; // Text ResultSet need End of metadata
        } else {
            needPacketCount = Packets.readLenEncAsInt(cumulateBuffer);
        }
        cumulateBuffer.readerIndex(payloadIndex + payloadLength); //avoid tail filler

        final boolean hasPacketNumber;
        hasPacketNumber = Packets.hasPacketNumber(cumulateBuffer, needPacketCount);

        cumulateBuffer.readerIndex(originalReaderIndex);
        return hasPacketNumber;
    }

    /**
     * <p>
     * Read column metadata from text protocol.
     * </p>
     */
    static MySQLRowMeta readForRows(final ByteBuf cumulateBuffer, final StmtTask stmtTask) {

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        stmtTask.updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));// update sequenceId

        final int payloadIndex = cumulateBuffer.readerIndex();
        final int columnCount = Packets.readLenEncAsInt(cumulateBuffer);
        cumulateBuffer.readerIndex(payloadIndex + payloadLength);//avoid tail filler

        final MySQLColumnMeta[] metaArray;
        metaArray = MySQLColumnMeta.readMetas(cumulateBuffer, columnCount, stmtTask);

        return new MySQLRowMeta(metaArray, stmtTask);
    }


    @Nullable
    static MySQLRowMeta readForPrepare(final ByteBuf cumulateBuffer, final int columnCount,
                                       final MetaAdjutant metaAdjutant) {
        final MySQLColumnMeta[] metaArray;
        metaArray = MySQLColumnMeta.readMetas(cumulateBuffer, columnCount, metaAdjutant);

        final MySQLRowMeta rowMeta;
        if (metaArray.length == 0) {
            rowMeta = EMPTY;
        } else {
            rowMeta = new MySQLRowMeta(metaArray);
        }
        return rowMeta;
    }

    private static final Option<Integer> COLLATION_INDEX = Option.from("COLUMN_COLLATION_INDEX", Integer.class);

    private static final Option<Charset> CHARSET = Option.from("COLUMN_CHARSET", Charset.class);

    /**
     * length of fixed length fields
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html"> Column Definition Protocol</a>
     */
    private static final Option<Long> FIXED_LENGTH = Option.from("COLUMN_FIXED_LENGTH", Long.class);

    /**
     * column_length
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html"> Column Definition Protocol</a>
     */
    private static final Option<Integer> LENGTH = Option.from("COLUMN_LENGTH", Integer.class);

    /**
     * flags
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html"> Column Definition Protocol</a>
     */
    private static final Option<Integer> FLAGS = Option.from("COLUMN_FLAGS", Integer.class);

    /**
     * virtual table name
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html"> Column Definition Protocol</a>
     */
    private static final Option<String> TABLE_LABEL = Option.from("COLUMN_TABLE_LABEL", String.class);

    private static final Option<Long> PRECISION = Option.from("COLUMN_PRECISION", Long.class);


    final MySQLColumnMeta[] columnMetaArray;

    final Map<Integer, Charsets.CustomCollation> customCollationMap;

    final Charset resultSetCharset;


    final ZoneOffset serverZone;

    private final Map<String, Integer> labelToIndexMap;


    /**
     * @see #readForPrepare(ByteBuf, int, MetaAdjutant)
     */
    private MySQLRowMeta(final MySQLColumnMeta[] columnMetaArray) {
        super(-1);
        this.columnMetaArray = columnMetaArray;
        this.customCollationMap = Collections.emptyMap();
        this.serverZone = PSEUDO_SERVER_ZONE;
        this.resultSetCharset = null;

        if (columnMetaArray.length < 6) {
            this.labelToIndexMap = null;
        } else {
            this.labelToIndexMap = createLabelToIndexMap(columnMetaArray);
        }

    }

    private MySQLRowMeta(final MySQLColumnMeta[] columnMetaArray, StmtTask stmtTask) {
        super(stmtTask.nextResultIndex());
        if (this.resultNo < 0) {
            throw new IllegalArgumentException("resultIndex must great than -1");
        }
        this.columnMetaArray = columnMetaArray;
        final TaskAdjutant adjutant = stmtTask.adjutant();
        this.customCollationMap = adjutant.obtainCustomCollationMap();
        this.serverZone = adjutant.serverZone();
        this.resultSetCharset = adjutant.getCharsetResults();

        if (columnMetaArray.length < 6) {
            this.labelToIndexMap = null;
        } else {
            this.labelToIndexMap = createLabelToIndexMap(columnMetaArray);
        }
    }

    @Override
    public int getColumnCount() {
        return this.columnMetaArray.length;
    }

    @Override
    public DataType getDataType(int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].sqlType;
    }


    @Override
    public JdbdType getJdbdType(final int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].sqlType.jdbdType();
    }

    @Override
    public FieldType getFieldType(final int indexBasedZero) throws JdbdException {
        final MySQLColumnMeta meta;
        meta = this.columnMetaArray[checkIndex(indexBasedZero)];
        final FieldType fieldType;
        if (MySQLStrings.hasText(meta.tableName)) {
            fieldType = FieldType.FIELD;
        } else {
            fieldType = FieldType.EXPRESSION;
        }
        return fieldType;
    }


    @Override
    public BooleanMode getAutoIncrementMode(final int indexBasedZero) throws JdbdException {
        final MySQLColumnMeta meta;
        meta = this.columnMetaArray[checkIndex(indexBasedZero)];
        final BooleanMode mode;
        if (meta.isAutoIncrement()) {
            mode = BooleanMode.TRUE;
        } else {
            mode = BooleanMode.FALSE;
        }
        return mode;
    }

    /**
     * <p>
     * jdbd-mysql support following options :
     *     <ul>
     *         <li>{@link #COLLATION_INDEX}</li>
     *         <li>{@link #CHARSET}</li>
     *         <li>{@link #FIXED_LENGTH}</li>
     *         <li>{@link #FLAGS}</li>
     *         <li>{@link #TABLE_LABEL}</li>
     *         <li>{@link #PRECISION}</li>
     *     </ul>
     * </p>
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getOf(final int indexBasedZero, final Option<T> option) throws JdbdException {
        final MySQLColumnMeta meta;
        meta = this.columnMetaArray[checkIndex(indexBasedZero)];

        final Object value;
        if (COLLATION_INDEX.equals(option)) {
            value = meta.collationIndex;
        } else if (CHARSET.equals(option)) {
            value = meta.columnCharset;
        } else if (FIXED_LENGTH.equals(option)) {
            value = meta.fixedLength;
        } else if (LENGTH.equals(option)) {
            value = meta.length;
        } else if (FLAGS.equals(option)) {
            value = meta.definitionFlags;
        } else if (TABLE_LABEL.equals(option)) {
            value = meta.tableLabel;
        } else if (PRECISION.equals(option)) {
            value = meta.obtainPrecision(this.customCollationMap);
        } else {
            value = null;
        }
        return (T) value;
    }

    @Override
    public String getCatalogName(final int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].catalogName;
    }

    @Override
    public String getSchemaName(final int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].schemaName;
    }

    @Override
    public String getTableName(final int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].tableName;
    }

    @Override
    public String getColumnName(final int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].columnName;
    }

    @Override
    public int getPrecision(final int indexBasedZero) throws JdbdException {
        final long precision;
        precision = this.columnMetaArray[checkIndex(indexBasedZero)].obtainPrecision(this.customCollationMap);
        final int actual;
        if (precision > Integer.MAX_VALUE) {
            actual = Integer.MAX_VALUE;
        } else {
            actual = (int) precision;
        }
        return actual;
    }

    @Override
    public int getScale(final int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].getScale();
    }

    @Override
    public KeyMode getKeyMode(final int indexBasedZero) throws JdbdException {
        final int flags;
        flags = this.columnMetaArray[checkIndex(indexBasedZero)].definitionFlags;
        final KeyMode mode;
        if ((flags & MySQLColumnMeta.PRI_KEY_FLAG) != 0) {
            mode = KeyMode.PRIMARY_KEY;
        } else if ((flags & MySQLColumnMeta.UNIQUE_KEY_FLAG) != 0) {
            mode = KeyMode.UNIQUE_KEY;
        } else if ((flags & MySQLColumnMeta.MULTIPLE_KEY_FLAG) != 0) {
            mode = KeyMode.MULTIPLE_KEY; // TODO check ?
        } else {
            mode = KeyMode.UNKNOWN;
        }
        return mode;
    }

    @Override
    public NullMode getNullMode(final int indexBasedZero) throws JdbdException {
        final int flags;
        flags = this.columnMetaArray[checkIndex(indexBasedZero)].definitionFlags;

        final NullMode mode;
        if ((flags & MySQLColumnMeta.NOT_NULL_FLAG) != 0) {
            mode = NullMode.NON_NULL;
        } else {
            mode = NullMode.NULLABLE;
        }
        return mode;
    }


    @Override
    public Class<?> getFirstJavaType(int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].sqlType.firstJavaType();
    }

    @Override
    public Class<?> getSecondJavaType(int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].sqlType.secondJavaType();
    }

    @Override
    public String getColumnLabel(int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].columnLabel;
    }

    @Override
    public int getColumnIndex(String columnLabel) throws JdbdException {
        final Map<String, Integer> labelToIndexMap = this.labelToIndexMap;
        if (labelToIndexMap != null) {
            final Integer columnIndex = labelToIndexMap.get(columnLabel);
            if (columnIndex == null) {
                throw createNotFoundIndexException(columnLabel);
            }
            return columnIndex;
        }

        int indexBasedZero = -1;
        final MySQLColumnMeta[] columnMetaArray = this.columnMetaArray;
        final int length = columnMetaArray.length;
        for (int i = length - 1; i > -1; i--) {
            if (columnLabel.equals(columnMetaArray[i].columnLabel)) {
                indexBasedZero = i;
                break;
            }
        }
        if (indexBasedZero < 0) {
            throw createNotFoundIndexException(columnLabel);
        }
        return indexBasedZero;
    }


    @Override
    protected ColumnMeta[] getColumnMetaArray() {
        return this.columnMetaArray;
    }


    int checkIndex(final int indexBaseZero) {
        if (indexBaseZero < 0 || indexBaseZero >= this.columnMetaArray.length) {
            String m = String.format("index[%s] out of bounds[0 -- %s].", indexBaseZero, columnMetaArray.length - 1);
            throw new JdbdException(m);
        }
        return indexBaseZero;
    }



}
