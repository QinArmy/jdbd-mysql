package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.meta.KeyMode;
import io.jdbd.meta.NullMode;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.FieldType;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.vendor.result.VendorResultRowMeta;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
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



    final MySQLColumnMeta[] columnMetaArray;

    final Map<Integer, Charsets.CustomCollation> customCollationMap;


    final ZoneOffset serverZone;

    int metaIndex = 0;


    /**
     * @see #readForPrepare(ByteBuf, int, MetaAdjutant)
     */
    private MySQLRowMeta(final MySQLColumnMeta[] columnMetaArray) {
        super(-1);
        this.columnMetaArray = columnMetaArray;
        this.customCollationMap = Collections.emptyMap();
        this.serverZone = PSEUDO_SERVER_ZONE;
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
    public String getTypeName(int indexBasedZero) throws JdbdException {
        return this.columnMetaArray[checkIndex(indexBasedZero)].sqlType.typeName();
    }

    @Override
    public JdbdType getJdbdType(int indexBasedZero) throws JdbdException {
        return null;
    }

    @Override
    public FieldType getFieldType(int indexBasedZero) throws JdbdException {
        return null;
    }

    @Override
    public boolean isSigned(int indexBasedZero) throws JdbdException {
        return false;
    }

    @Override
    public boolean getAutoIncrementMode(int indexBasedZero) throws JdbdException {
        return false;
    }

    @Override
    public String getCatalogName(int indexBasedZero) throws JdbdException {
        return null;
    }

    @Override
    public String getSchemaName(int indexBasedZero) throws JdbdException {
        return null;
    }

    @Override
    public String getTableName(int indexBasedZero) throws JdbdException {
        return null;
    }

    @Override
    public String getColumnName(int indexBasedZero) throws JdbdException {
        return null;
    }

    @Override
    public int getPrecision(int indexBasedZero) throws JdbdException {
        return 0;
    }

    @Override
    public int getScale(int indexBasedZero) throws JdbdException {
        return 0;
    }

    @Override
    public KeyMode getKeyMode(int indexBasedZero) throws JdbdException {
        return null;
    }

    @Override
    public NullMode getNullMode(int indexBasedZero) throws JdbdException {
        return null;
    }


    @Override
    public Class<?> getFirstJavaType(int indexBasedZero) throws JdbdException {
        return null;
    }

    @Override
    public Class<?> getSecondJavaType(int indexBasedZero) throws JdbdException {
        return null;
    }

    @Override
    public List<String> getColumnLabelList() {
        return null;
    }

    @Override
    public String getColumnLabel(int indexBasedZero) throws JdbdException {
        return null;
    }

    @Override
    public int getColumnIndex(String columnLabel) throws JdbdException {
        return 0;
    }

    boolean isReady() {
        return this.metaIndex == this.columnMetaArray.length;
    }

    int convertToIndex(String columnAlias) throws JdbdException {
        MySQLColumnMeta[] columnMetas = this.columnMetaArray;
        int len = columnMetas.length;
        for (int i = 0; i < len; i++) {
            if (columnMetas[i].columnLabel.equals(columnAlias)) {
                return i;
            }
        }
        throw new JdbdException(String.format("Not found column for columnAlias[%s]", columnAlias));
    }

    final MySQLType getMySQLType(int indexBaseZero) {
        return this.columnMetaArray[checkIndex(indexBaseZero)].sqlType;
    }

    public final Charset getColumnCharset(int indexBaseZero) {
        return this.columnMetaArray[checkIndex(indexBaseZero)].columnCharset;
    }

    int checkIndex(final int indexBaseZero) {
        if (indexBaseZero < 0 || indexBaseZero >= this.columnMetaArray.length) {
            String m = String.format("index[%s] out of bounds[0 -- %s].", indexBaseZero, columnMetaArray.length - 1);
            throw new JdbdException(m);
        }
        return indexBaseZero;
    }

    private boolean doIsCaseSensitive(MySQLColumnMeta columnMeta) {
        boolean caseSensitive;
        switch (columnMeta.sqlType) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
            case INT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case BIGINT:
            case BIGINT_UNSIGNED:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DATE:
            case YEAR:
            case TIME:
            case TIMESTAMP:
            case DATETIME:
                caseSensitive = false;
                break;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
            case SET:
                String collationName = Charsets.getCollationNameByIndex(columnMeta.collationIndex);
                caseSensitive = ((collationName != null) && !collationName.endsWith("_ci"));
                break;
            default:
                caseSensitive = true;
        }
        return caseSensitive;
    }


}
