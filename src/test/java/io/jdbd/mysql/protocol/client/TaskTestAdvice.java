package io.jdbd.mysql.protocol.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jdbd.meta.DataType;
import io.jdbd.meta.NullMode;
import io.jdbd.mysql.Groups;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.type.City;
import io.jdbd.mysql.type.TrueOrFalse;
import io.jdbd.mysql.util.MySQLStreams;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.stmt.Stmts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static org.testng.Assert.*;

@Test(groups = {Groups.DATA_PREPARE}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS})
public class TaskTestAdvice extends AbstractTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTaskSuiteTests.class);


    @AfterSuite(timeOut = 10 * 1000)
    public static void afterSuite(ITestContext context) {
        Flux.fromIterable(TASK_ADJUTANT_QUEUE)
                .flatMap(QuitTask::quit)
                .then()
                .block();

    }

    @Test(timeOut = TIME_OUT)
    public void prepareData() throws Exception {
        LOG.info("\n {} group test start.\n", Groups.DATA_PREPARE);

        final TaskAdjutant adjutant = obtainTaskAdjutant();

        final Path path = Paths.get(ClientTestUtils.getTestResourcesPath().toString(), "script/ddl/mysqlTypes.sql");

        final List<String> commandList = new ArrayList<>(2);
        commandList.add("DROP TABLE IF EXISTS mysql_types");
        commandList.add(MySQLStreams.readAsString(path));

        LOG.info("start create mysql_types table.");

        // single statement mode batch update
        final List<ResultStates> resultStatesList;
        resultStatesList = ComQueryTask.batchUpdate(Stmts.batch(commandList), adjutant)
                .collectList()
                .block();

        assertNotNull(resultStatesList, "resultStatesList");
        assertEquals(resultStatesList.size(), 2, "resultStatesList size");

        LOG.info("have truncated mysql_types");

        doPrepareData(adjutant);

        LOG.info("create mysql_types table success");
        releaseConnection(adjutant);
    }

    @Test(enabled = false, dependsOnMethods = "prepareData")
    public void mysqlTypeMetadataMatch() {
        LOG.info("mysqlTypeMatch test start");
        final TaskAdjutant adjutant = obtainTaskAdjutant();

        List<ResultRow> resultRowList = ComQueryTask.query(Stmts.stmt(createQuerySqlForMySQLTypeMatch()), DatabaseProtocol.ROW_FUNC, adjutant)
                .collectList()
                .doOnError(this::printMultiError)
                .block();

        assertNotNull(resultRowList, "resultRowList");
        assertFalse(resultRowList.isEmpty(), "resultRowList is empty");

        final Environment env = adjutant.host().properties();
        for (ResultRow resultRow : resultRowList) {
            assertQueryResultRowMySQLTypeMatch(resultRow, env);
        }

        LOG.info("mysqlTypeMatch test success");
        releaseConnection(adjutant);
    }

    /*################################## blow private method ##################################*/

    private static void doPrepareData(TaskAdjutant taskAdjutant) throws Exception {
        final int rowCount = 10000;

        StringBuilder builder = new StringBuilder(40 * rowCount)
                .append("INSERT INTO mysql_types(my_var_char200,my_char200,my_bit64,my_boolean,my_json) VALUES");

        final Random random = new Random();

        final ObjectMapper mapper = new ObjectMapper();

        for (int i = 1; i <= rowCount; i++) {
            if (i > 1) {
                builder.append(",");
            }
            builder.append("(")
                    //.append(i)//id
                    .append("'zoro")//name
                    .append(i)
                    .append("','simonyi")//my_char
                    .append(i)
                    .append("',B'")
                    .append(Long.toBinaryString(random.nextLong()))
                    .append("',TRUE")
                    .append(",'")
                    .append(mapper.writeValueAsString(Collections.singletonMap("name", "zoro")))
                    .append("')");
        }

        final String command = builder.toString();
        // LOG.info("prepare data command:\n {}", builder.toString());
//        byte[] bytes = command.getBytes(taskAdjutant.obtainCharsetClient());
//        LOG.info("prepare data command bytes:{}, times:{}",bytes.length,bytes.length / PacketUtils.MAX_PAYLOAD);
//
        ResultStates resultStates = ComQueryTask.update(Stmts.stmt(command), taskAdjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.affectedRows(), rowCount, "affectedRows");
        assertFalse(resultStates.hasMoreResult(), "hasMoreResults");
        LOG.info("prepared data rows:{}", resultStates.affectedRows());
        LOG.info("InsertId:{}", resultStates.lastInsertedId());

    }

    /**
     * @see #mysqlTypeMetadataMatch()
     */
    private String createQuerySqlForMySQLTypeMatch() {
        StringBuilder builder = MySQLStrings.builder();
        builder.append("SELECT")
                .append(" t.id as id")
                .append(",t.create_time as createTime")
                .append(",t.update_time as updateTime")
                .append(",t.name as name")

                .append(",t.my_char as myChar")
                .append(",t.my_binary as myBinary")
                .append(",t.my_var_binary as myVarBinary")
                .append(",t.my_bit as myBit")

                .append(",t.my_tinyint1 as myTinyint1")
                .append(",t.my_tinyint as myTinyint")
                .append(",t.my_tinyint_unsigned as myTinyintUnsigned")
                .append(",t.my_boolean as myBoolean")

                .append(",t.my_smallint as mySmallint")
                .append(",t.my_smallint_unsigned as mySmallintUnsigned")
                .append(",t.my_mediumint as myMediumint")
                .append(",t.my_mediumint_unsigned as myMediumintUnsigned")

                .append(",t.my_int as myInt")
                .append(",t.my_int_unsigned as myIntUnsigned")
                .append(",t.my_bigint_unsigned as myBigintUnsigned")
                .append(",t.my_decimal as myDecimal")

                .append(",t.my_decimal_unsigned as myDecimalUnsigned")
                .append(",t.my_float as myFloat")
                .append(",t.my_float_unsigned as myFloatUnsigned")
                .append(",t.my_double as myDouble")

                .append(",t.my_double_unsigned as myDoubleUnsigned")
                .append(",t.my_enum as myEnum")
                .append(",t.my_set as mySet")
                .append(",t.my_json as myJson")

                .append(",t.my_geometry as myGeometry")
                .append(",t.my_point as myPoint")
                .append(",t.my_linestring as myLinestring")
                .append(",t.my_polygon as myPolygon")

                .append(",t.my_multipoint as myMultipoint")
                .append(",t.my_multilinestring as myMultilinestring")
                .append(",t.my_multipolygon as myMultipolygon")
                .append(",t.my_geometrycollection as myGeometrycollection")

                .append(",t.my_timestamp as myTimestamp")
                .append(",t.my_timestamp1 as myTimestamp1")
                .append(",t.my_date as myDate")
                .append(",t.my_time as myTime")

                .append(",t.my_time1 as myTime1")
                .append(",t.my_bit20 as myBit20")
                .append(",t.my_tiny_text as myTinyText")
                .append(",t.my_text as myText")

                .append(",t.my_medium_text as myMediumText")
                .append(",t.my_long_text as myLongText")
                .append(",t.my_tinyblob as myTinyBlob")
                .append(",t.my_blob as myBlob")

                .append(",t.my_medium_blob as myMediumBlob")
                .append(",t.my_long_blob as myLongBlob");

        return builder
                .append(" FROM mysql_types as t ORDER BY t.id DESC LIMIT 10")
                .toString();
    }


    private void assertQueryResultRowMySQLTypeMatch(final ResultRow row, final Environment properties) {
        final ResultRowMeta rowMeta = row.getRowMeta();

        final Object id = row.get("id");
        assertNotNull(id, "id");
        assertTrue(id instanceof Long, "id java class");
        assertEquals(rowMeta.getDataType("id"), MySQLType.BIGINT, "id mysql type");
        assertFalse(rowMeta.getJdbdType("id").isUnsigned(), "id isUnsigned");
        assertEquals(rowMeta.getNullMode("id"), NullMode.NON_NULL, "id null mode.");
        assertTrue(rowMeta.getAutoIncrementMode("id"), "id isAutoIncrement");


        final LocalDateTime createTime = row.get("createTime", LocalDateTime.class);
        assertNotNull(createTime, "createTime");
        assertEquals(rowMeta.getDataType("createTime"), MySQLType.DATETIME, "createTime mysql type");
        assertEquals(rowMeta.getPrecision("createTime"), 0L, "createTime precision");
        assertEquals(rowMeta.getNullMode("createTime"), NullMode.NON_NULL, "createTime null mode.");

        final LocalDateTime updateTime = row.get("updateTime", LocalDateTime.class);
        assertNotNull(updateTime, "updateTime");
        assertEquals(rowMeta.getDataType("updateTime"), MySQLType.DATETIME, "updateTime mysql type");
        assertEquals(rowMeta.getPrecision("updateTime"), 6L, "updateTime precision");
        assertEquals(rowMeta.getNullMode("updateTime"), NullMode.NON_NULL, "updateTime null mode.");

        final String name = row.get("name", String.class);
        assertNotNull(name, "name");
        assertEquals(rowMeta.getDataType("name"), MySQLType.VARCHAR, "name mysql type");
        assertEquals(rowMeta.getNullMode("name"), NullMode.NON_NULL, "name null mode.");

        final String myChar = row.get("myChar", String.class);
        assertNotNull(myChar, "myChar");
        assertEquals(rowMeta.getDataType("myChar"), MySQLType.CHAR, "myChar mysql type");
        assertEquals(rowMeta.getNullMode("myChar"), NullMode.NON_NULL, "myChar null mode.");

        final byte[] myBinary = row.get("myBinary", byte[].class);
        assertNotNull(myBinary, "myBinary");
        assertEquals(rowMeta.getDataType("myBinary"), MySQLType.BINARY, "myBinary mysql type");
        assertEquals(rowMeta.getNullMode("myBinary"), NullMode.NON_NULL, "myBinary null mode.");

        final byte[] myVarBinary = row.get("myVarBinary", byte[].class);
        assertNotNull(myVarBinary, "myVarBinary");
        assertEquals(rowMeta.getDataType("myVarBinary"), MySQLType.VARBINARY, "myVarBinary mysql type");
        assertEquals(rowMeta.getNullMode("myVarBinary"), NullMode.NON_NULL, "myVarBinary null mode.");

        final Long myBit = row.get("myBit", Long.class);
        assertNotNull(myBit, "myBit");
        assertEquals(rowMeta.getDataType("myBit"), MySQLType.BIT, "myBit mysql type");
        assertEquals(Long.toBinaryString(myBit), row.get("myBit", String.class), "myBit string");
        assertEquals(rowMeta.getPrecision("myBit"), 64, "myBit precision");
        assertEquals(rowMeta.getNullMode("myBit"), NullMode.NON_NULL, "myBit null mode.");

        final Byte myTinyint1 = row.get("myTinyint1", Byte.class);
        assertNotNull(myTinyint1, "myTinyint1");
        assertTinyInt1Type(row, "myTinyint1", properties);
        assertFalse(rowMeta.getJdbdType("myTinyint1").isUnsigned(), "myTinyint1 isUnsigned");
        assertEquals(rowMeta.getNullMode("myTinyint1"), NullMode.NON_NULL, "myTinyint1 null mode.");

        final Byte myTinyint = row.get("myTinyint", Byte.class);
        assertNotNull(myTinyint, "myTinyint");
        assertEquals(rowMeta.getDataType("myTinyint"), MySQLType.TINYINT, "myTinyint mysql type");
        assertFalse(rowMeta.getJdbdType("myTinyint").isUnsigned(), "myTinyint isUnsigned");
        assertEquals(rowMeta.getNullMode("myTinyint"), NullMode.NON_NULL, "myTinyint null mode.");

        // below tiny_unsigned assert
        final Integer myTinyintUnsigned = row.get("myTinyintUnsigned", Integer.class);
        assertNotNull(myTinyintUnsigned, "myTinyintUnsigned");
        assertEquals(rowMeta.getDataType("myTinyintUnsigned"), MySQLType.TINYINT_UNSIGNED, "myTinyintUnsigned mysql type");
        assertTrue(rowMeta.getJdbdType("myTinyintUnsigned").isUnsigned(), "myTinyintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myTinyintUnsigned"), NullMode.NON_NULL, "myTinyintUnsigned null mode.");

        // below boolean assert
        final Object myBoolean = row.get("myBoolean");
        assertNotNull(myBoolean, "myBoolean");
        assertTinyInt1Type(row, "myBoolean", properties);
        assertEquals(rowMeta.getNullMode("myBoolean"), NullMode.NON_NULL, "myBoolean null mode.");

        // below smallint assert
        final Object mySmallint = row.get("mySmallint");
        assertNotNull(mySmallint, "mySmallint");
        assertTrue(mySmallint instanceof Short, "mySmallint java class.");
        assertEquals(rowMeta.getDataType("mySmallint"), MySQLType.SMALLINT, "mySmallint mysql type");
        assertFalse(rowMeta.getJdbdType("mySmallint").isUnsigned(), "mySmallint isUnsigned");
        assertEquals(rowMeta.getNullMode("mySmallint"), NullMode.NON_NULL, "mySmallint null mode.");

        // below smallint_unsigned assert
        final Object mySmallintUnsigned = row.get("mySmallintUnsigned");
        assertNotNull(mySmallintUnsigned, "mySmallintUnsigned");
        assertTrue(mySmallintUnsigned instanceof Integer, "mySmallintUnsigned java class.");
        assertEquals(rowMeta.getDataType("mySmallintUnsigned"), MySQLType.SMALLINT_UNSIGNED, "mySmallintUnsigned mysql type");
        assertTrue(rowMeta.getJdbdType("mySmallintUnsigned").isUnsigned(), "mySmallintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("mySmallintUnsigned"), NullMode.NON_NULL, "mySmallintUnsigned null mode.");

        // below mediumint assert
        final Object myMediumint = row.get("myMediumint");
        assertNotNull(myMediumint, "myMediumint");
        assertTrue(myMediumint instanceof Integer, "myMediumint java class.");
        assertEquals(rowMeta.getDataType("myMediumint"), MySQLType.MEDIUMINT, "myMediumint mysql type");
        assertFalse(rowMeta.getJdbdType("myMediumint").isUnsigned(), "myMediumint isUnsigned");
        assertEquals(rowMeta.getNullMode("myMediumint"), NullMode.NON_NULL, "myMediumint null mode.");

        // below mediumint_unsigned  assert
        final Object myMediumintUnsigned = row.get("myMediumintUnsigned");
        assertNotNull(myMediumintUnsigned, "myMediumintUnsigned");
        assertTrue(myMediumintUnsigned instanceof Integer, "myMediumintUnsigned java class.");
        assertEquals(rowMeta.getDataType("myMediumintUnsigned"), MySQLType.MEDIUMINT_UNSIGNED, "myMediumintUnsigned mysql type");
        assertTrue(rowMeta.getJdbdType("myMediumintUnsigned").isUnsigned(), "myMediumintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myMediumintUnsigned"), NullMode.NON_NULL, "myMediumintUnsigned null mode.");

        // blow int assert
        final Object myInt = row.get("myInt");
        assertNotNull(myInt, "myInt");
        assertTrue(myInt instanceof Integer, "myInt java class.");
        assertEquals(rowMeta.getDataType("myInt"), MySQLType.INT, "myInt mysql type");
        assertFalse(rowMeta.getJdbdType("myInt").isUnsigned(), "myInt isUnsigned");
        assertEquals(rowMeta.getNullMode("myInt"), NullMode.NON_NULL, "myInt null mode.");

        // below int_unsigned assert
        final Object myIntUnsigned = row.get("myIntUnsigned");
        assertNotNull(myIntUnsigned, "myIntUnsigned");
        assertTrue(myIntUnsigned instanceof Long, "myIntUnsigned java class.");
        assertEquals(rowMeta.getDataType("myIntUnsigned"), MySQLType.INT_UNSIGNED, "myIntUnsigned mysql type");
        assertTrue(rowMeta.getJdbdType("myIntUnsigned").isUnsigned(), "myIntUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myIntUnsigned"), NullMode.NON_NULL, "myIntUnsigned null mode.");

        // below bigint_unsigned assert
        final Object myBigintUnsigned = row.get("myBigintUnsigned");
        assertNotNull(myBigintUnsigned, "myBigintUnsigned");
        assertTrue(myBigintUnsigned instanceof BigInteger, "myBigintUnsigned java class.");
        assertEquals(rowMeta.getDataType("myBigintUnsigned"), MySQLType.BIGINT_UNSIGNED, "myBigintUnsigned mysql type");
        assertTrue(rowMeta.getJdbdType("myBigintUnsigned").isUnsigned(), "myBigintUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myBigintUnsigned"), NullMode.NON_NULL, "myBigintUnsigned null mode.");

        // below decimal assert
        final Object myDecimal = row.get("myDecimal");
        assertNotNull(myDecimal, "myDecimal");
        assertTrue(myDecimal instanceof BigDecimal, "myDecimal java class.");
        assertEquals(rowMeta.getDataType("myDecimal"), MySQLType.DECIMAL, "myDecimal mysql type");
        assertFalse(rowMeta.getJdbdType("myDecimal").isUnsigned(), "myDecimal isUnsigned");
        assertEquals(rowMeta.getPrecision("myDecimal"), 14L, "myDecimal precision");
        assertEquals(rowMeta.getScale("myDecimal"), 2, "myDecimal scale");
        assertEquals(rowMeta.getNullMode("myDecimal"), NullMode.NON_NULL, "myDecimal null mode.");

        // below decimal_unsigned
        final Object myDecimalUnsigned = row.get("myDecimalUnsigned");
        assertNotNull(myDecimalUnsigned, "myDecimalUnsigned");
        assertTrue(myDecimalUnsigned instanceof BigDecimal, "myDecimalUnsigned java class.");
        assertEquals(rowMeta.getDataType("myDecimalUnsigned"), MySQLType.DECIMAL_UNSIGNED, "myDecimalUnsigned mysql type");
        assertTrue(rowMeta.getJdbdType("myDecimalUnsigned").isUnsigned(), "myDecimalUnsigned isUnsigned");
        assertEquals(rowMeta.getPrecision("myDecimalUnsigned"), 14L, "myDecimalUnsigned precision");
        assertEquals(rowMeta.getScale("myDecimalUnsigned"), 2, "myDecimalUnsigned scale");
        assertEquals(rowMeta.getNullMode("myDecimalUnsigned"), NullMode.NON_NULL, "myDecimalUnsigned null mode.");

        // below float assert
        final Object myFloat = row.get("myFloat");
        assertNotNull(myFloat, "myFloat");
        assertTrue(myFloat instanceof Float, "myFloat java class.");
        assertEquals(rowMeta.getDataType("myFloat"), MySQLType.FLOAT, "myFloat mysql type");
        assertFalse(rowMeta.getJdbdType("myFloat").isUnsigned(), "myFloat isUnsigned");
        assertEquals(rowMeta.getNullMode("myFloat"), NullMode.NON_NULL, "myFloat null mode.");

        // below float_unsigned
        final Object myFloatUnsigned = row.get("myFloatUnsigned");
        assertNotNull(myFloatUnsigned, "myFloatUnsigned");
        assertSame(myFloatUnsigned.getClass(), MySQLType.FLOAT_UNSIGNED.firstJavaType(), "myFloatUnsigned java class.");
        assertEquals(rowMeta.getDataType("myFloatUnsigned"), MySQLType.FLOAT_UNSIGNED, "myFloatUnsigned mysql type");
        assertTrue(rowMeta.getJdbdType("myFloatUnsigned").isUnsigned(), "myFloatUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myFloatUnsigned"), NullMode.NON_NULL, "myFloatUnsigned null mode.");

        // below double assert
        final Object myDouble = row.get("myDouble");
        assertNotNull(myDouble, "myDouble");
        assertSame(myDouble.getClass(), MySQLType.DOUBLE_UNSIGNED.firstJavaType(), "myDouble java class.");
        assertEquals(rowMeta.getDataType("myDouble"), MySQLType.DOUBLE, "myDouble mysql type");
        assertFalse(rowMeta.getJdbdType("myDouble").isUnsigned(), "myDouble isUnsigned");
        assertEquals(rowMeta.getNullMode("myDouble"), NullMode.NON_NULL, "myDouble null mode.");

        // below double_unsigned assert
        final Object myDoubleUnsigned = row.get("myDoubleUnsigned");
        assertNotNull(myDoubleUnsigned, "myDoubleUnsigned");
        assertSame(myDoubleUnsigned.getClass(), MySQLType.DOUBLE_UNSIGNED.firstJavaType(), "myDoubleUnsigned java class.");
        assertEquals(rowMeta.getDataType("myDoubleUnsigned"), MySQLType.DOUBLE_UNSIGNED, "myDoubleUnsigned mysql type");
        assertTrue(rowMeta.getJdbdType("myDoubleUnsigned").isUnsigned(), "myDoubleUnsigned isUnsigned");
        assertEquals(rowMeta.getNullMode("myDoubleUnsigned"), NullMode.NON_NULL, "myDoubleUnsigned null mode.");

        // below enum assert
        final Object myEnum = row.get("myEnum");
        assertNotNull(myEnum, "myEnum");
        assertTrue(myEnum instanceof String, "myEnum java class.");
        final TrueOrFalse trueOrFalse = row.get("myEnum", TrueOrFalse.class);
        assertNotNull(trueOrFalse, "myEnum");
        assertEquals(rowMeta.getDataType("myEnum"), MySQLType.ENUM, "myEnum mysql type");
        assertFalse(rowMeta.getJdbdType("myEnum").isUnsigned(), "myEnum isUnsigned");
        assertEquals(rowMeta.getNullMode("myEnum"), NullMode.NON_NULL, "myEnum null mode.");

        // below set type assert

        final Object mySet = row.get("mySet");
        assertNotNull(myEnum, "mySet");
        assertTrue(mySet instanceof Set, "mySet java class.");
        assertEquals(rowMeta.getDataType("mySet"), MySQLType.SET, "mySet mysql type");
        assertFalse(rowMeta.getJdbdType("mySet").isUnsigned(), "mySet isUnsigned");
        assertEquals(rowMeta.getNullMode("mySet"), NullMode.NON_NULL, "mySet null mode.");

        @SuppressWarnings("unchecked") final Set<String> stringSet = (Set<String>) mySet;
        final Set<City> citySet = row.getSet("mySet", City.class);
        final List<City> cityList = row.getList("mySet", City.class);
        assertNotNull(citySet, "citySet");
        assertNotNull(cityList, "cityList");
        assertEquals(citySet.size(), stringSet.size(), "citySet size");
        assertEquals(cityList.size(), stringSet.size(), "cityList size");

        for (City city : citySet) {
            if (!stringSet.contains(city.name())) {
                fail(String.format("city[%s] not exits.", city));
            }
        }

        for (City city : cityList) {
            if (!stringSet.contains(city.name())) {
                fail(String.format("city[%s] not exits.", city));
            }
        }


        // below json type assert
        final Object myJson = row.get("myJson");
        assertNotNull(myJson, "myJson");
        assertTrue(myJson instanceof String, "myJson java class.");
        assertNotNull(trueOrFalse, "myJson");
        assertEquals(rowMeta.getDataType("myJson"), MySQLType.JSON, "myJson mysql type");
        assertFalse(rowMeta.getJdbdType("myJson").isUnsigned(), "myJson isUnsigned");
        assertEquals(rowMeta.getNullMode("myJson"), NullMode.NULLABLE, "myJson null mode.");

        // below geometry type assert
        assertEquals(rowMeta.getDataType("myGeometry"), MySQLType.GEOMETRY, "myGeometry mysql type");
        assertFalse(rowMeta.getJdbdType("myGeometry").isUnsigned(), "myGeometry isUnsigned");
        assertEquals(rowMeta.getNullMode("myGeometry"), NullMode.NULLABLE, "myGeometry null mode.");

        // below point type assert
        assertEquals(rowMeta.getDataType("myPoint"), MySQLType.GEOMETRY, "myPoint mysql type");
        assertEquals(rowMeta.getNullMode("myPoint"), NullMode.NULLABLE, "myPoint null mode.");


        // below linestring type assert
        assertEquals(rowMeta.getDataType("myLinestring"), MySQLType.GEOMETRY, "myLinestring mysql type");
        assertEquals(rowMeta.getNullMode("myLinestring"), NullMode.NULLABLE, "myLinestring null mode.");

        // below polygon type assert
        assertEquals(rowMeta.getDataType("myPolygon"), MySQLType.GEOMETRY, "myPolygon mysql type");
        assertEquals(rowMeta.getNullMode("myPolygon"), NullMode.NULLABLE, "myPolygon null mode.");

        // below multipoint type assert
        assertEquals(rowMeta.getDataType("myMultipoint"), MySQLType.GEOMETRY, "myMultipoint mysql type");
        assertEquals(rowMeta.getNullMode("myMultipoint"), NullMode.NULLABLE, "myMultipoint null mode.");

        // below multilinestring type assert
        assertEquals(rowMeta.getDataType("myMultilinestring"), MySQLType.GEOMETRY, "myMultilinestring mysql type");
        assertEquals(rowMeta.getNullMode("myMultilinestring"), NullMode.NULLABLE, "myMultilinestring null mode.");

        // below multipolygon type assert
        assertEquals(rowMeta.getDataType("myMultipolygon"), MySQLType.GEOMETRY, "myMultipolygon mysql type");
        assertEquals(rowMeta.getNullMode("myMultipolygon"), NullMode.NULLABLE, "myMultipolygon null mode.");

        // below geometrycollection type assert
        assertEquals(rowMeta.getDataType("myGeometrycollection"), MySQLType.GEOMETRY, "myGeometrycollection mysql type");
        assertEquals(rowMeta.getNullMode("myGeometrycollection"), NullMode.NULLABLE, "myGeometrycollection null mode.");

        final LocalDateTime myTimestamp = row.get("myTimestamp", LocalDateTime.class);
        assertNotNull(myTimestamp, "myTimestamp");
        assertEquals(rowMeta.getDataType("myTimestamp"), MySQLType.TIMESTAMP, "myTimestamp mysql type");
        assertEquals(rowMeta.getPrecision("myTimestamp"), 0, "myTimestamp precision");
        assertEquals(rowMeta.getNullMode("myTimestamp"), NullMode.NON_NULL, "myTimestamp null mode.");

        final Object myTimestamp1 = row.get("myTimestamp1");
        assertNotNull(myTimestamp1, "myTimestamp1");
        assertEquals(row.get("myTimestamp1", LocalDateTime.class), myTimestamp1, "myTimestamp1 convert");
        assertEquals(rowMeta.getDataType("myTimestamp1"), MySQLType.TIMESTAMP, "myTimestamp1 mysql type");
        assertEquals(rowMeta.getPrecision("myTimestamp1"), 1L, "myTimestamp1 precision");
        assertEquals(rowMeta.getNullMode("myTimestamp1"), NullMode.NON_NULL, "myTimestamp1 null mode.");

        final Object myDate = row.get("myDate");
        assertNotNull(myDate, "myDate");
        assertEquals(row.get("myDate", LocalDate.class), myDate, "myDate convert");
        assertEquals(rowMeta.getDataType("myDate"), MySQLType.DATE, "myDate mysql type");
        assertEquals(rowMeta.getPrecision("myDate"), -1, "myDate precision");
        assertEquals(rowMeta.getNullMode("myDate"), NullMode.NON_NULL, "myDate null mode.");

        final Object myTime = row.get("myTime");
        assertNotNull(myTime, "myTime");
        assertEquals(row.get("myTime", LocalTime.class), myTime, "myTime convert");
        assertEquals(rowMeta.getDataType("myTime"), MySQLType.TIME, "myTime mysql type");
        assertEquals(rowMeta.getPrecision("myTime"), 0L, "myTime precision");
        assertEquals(rowMeta.getNullMode("myTime"), NullMode.NON_NULL, "myTime null mode.");

        final Object myTime1 = row.get("myTime1");
        assertNotNull(myTime1, "myTime1");
        assertEquals(row.get("myTime1", LocalTime.class), myTime1, "myTime1 convert");
        assertEquals(rowMeta.getDataType("myTime1"), MySQLType.TIME, "myTime1 mysql type");
        assertEquals(rowMeta.getPrecision("myTime1"), 1L, "myTime1 precision");
        assertEquals(rowMeta.getNullMode("myTime1"), NullMode.NON_NULL, "myTime1 null mode.");


        final Object myBit20 = row.get("myBit20");
        assertNotNull(myBit20, "myBit20");
        assertEquals(row.get("myBit20", Long.class), myBit20, "myBit20 convert");
        assertEquals(rowMeta.getDataType("myBit20"), MySQLType.BIT, "myBit20 mysql type");
        assertEquals(Long.toBinaryString((Long) myBit20), row.get("myBit20", String.class), "myBit20 string");
        assertEquals(rowMeta.getPrecision("myBit20"), 20, "myBit20 precision");
        assertEquals(rowMeta.getNullMode("myBit20"), NullMode.NON_NULL, "myBit20 null mode.");


        assertEquals(rowMeta.getDataType("myTinyBlob"), MySQLType.TINYBLOB, "myTinyBlob mysql type");
        // assertEquals(rowMeta.getPrecision("myTinyBlob"), (1L << 8) - 1L, "myTinyBlob precision");
        assertEquals(rowMeta.getScale("myTinyBlob"), -1, "myTinyBlob scale");
        assertEquals(rowMeta.getNullMode("myTinyBlob"), NullMode.NULLABLE, "myTinyBlob null mode.");


        assertEquals(rowMeta.getDataType("myBlob"), MySQLType.BLOB, "myBlob mysql type");
        // assertEquals(rowMeta.getPrecision("myBlob"), (1L << 16) - 1L, "myBlob precision");
        assertEquals(rowMeta.getNullMode("myBlob"), NullMode.NULLABLE, "myBlob null mode.");

        assertEquals(rowMeta.getDataType("myMediumBlob"), MySQLType.MEDIUMBLOB, "myMediumBlob mysql type");
        //assertEquals(rowMeta.getPrecision("myMediumBlob"), (1L << 24) - 1L, "myMediumBlob precision");
        assertEquals(rowMeta.getNullMode("myMediumBlob"), NullMode.NULLABLE, "myMediumBlob null mode.");

        assertEquals(rowMeta.getDataType("myLongBlob"), MySQLType.LONGBLOB, "myLongBlob mysql type");
        // assertEquals(rowMeta.getPrecision("myLongBlob"), (1L << 32) - 1L, "myLongBlob precision");
        assertEquals(rowMeta.getNullMode("myLongBlob"), NullMode.NULLABLE, "myLongBlob null mode.");

        assertEquals(rowMeta.getDataType("myTinyText"), MySQLType.TINYTEXT, "myTinyText mysql type");
        // assertEquals(rowMeta.getPrecision("myTinyText"), (1L << 8) - 1L, "myTinyText precision");
        assertEquals(rowMeta.getScale("myTinyText"), -1, "myTinyText scale");
        assertEquals(rowMeta.getNullMode("myTinyText"), NullMode.NULLABLE, "myTinyText null mode.");

        assertEquals(rowMeta.getDataType("myText"), MySQLType.TEXT, "myText mysql type");
        //assertEquals(rowMeta.getPrecision("myText"), (1L << 16) - 1L, "myText precision");
        assertEquals(rowMeta.getScale("myText"), -1, "myText scale");
        assertEquals(rowMeta.getNullMode("myText"), NullMode.NULLABLE, "myText null mode.");

        assertEquals(rowMeta.getDataType("myMediumText"), MySQLType.MEDIUMTEXT, "myMediumText mysql type");
        //assertEquals(rowMeta.getPrecision("myMediumText"), (1L << 24) - 1L, "myMediumText precision");
        assertEquals(rowMeta.getNullMode("myMediumText"), NullMode.NULLABLE, "myMediumText null mode.");

        assertEquals(rowMeta.getDataType("myLongText"), MySQLType.LONGTEXT, "myLongText mysql type");
        // assertEquals(rowMeta.getPrecision("myLongText"), (1L << 32) - 1L, "myLongText precision");
        assertEquals(rowMeta.getNullMode("myLongText"), NullMode.NULLABLE, "myLongText null mode.");

    }


    private void printMultiError(Throwable e) {
        if (e instanceof JdbdCompositeException) {
            int index = 0;
            for (Throwable throwable : ((JdbdCompositeException) e).getErrorList()) {
                LOG.error("multi error:{} :\n", index, throwable);
                index++;
            }
        }


    }


    private void assertTinyInt1Type(ResultRow row, String columnAlias, Environment properties) {
        final ResultRowMeta rowMeta = row.getRowMeta();

        assertFalse(rowMeta.getJdbdType(columnAlias).isUnsigned(), columnAlias + " isSigned");
        final DataType myBooleanType = rowMeta.getDataType(columnAlias);
        assertNotNull(row.get(columnAlias, Boolean.class), columnAlias + " convert to boolean");

        if (properties.isOn(MySQLKey.TINY_INT1_IS_BIT)) {
            if (properties.isOn(MySQLKey.TRANS_FORMED_BIT_IS_BOOLEAN)) {
                assertEquals(myBooleanType, MySQLType.BOOLEAN, columnAlias + " mysql type");
                assertTrue(row.get(columnAlias) instanceof Boolean, columnAlias + " is boolean type.");
            } else {
                assertEquals(myBooleanType, MySQLType.BIT, columnAlias + " mysql type");
                assertTrue(row.get(columnAlias) instanceof Long, columnAlias + " is Byte type.");
            }

        } else {
            assertEquals(myBooleanType, MySQLType.TINYINT, columnAlias + " mysql type");
            assertTrue(row.get(columnAlias) instanceof Byte, columnAlias + " is Byte type.");
        }

    }


}
