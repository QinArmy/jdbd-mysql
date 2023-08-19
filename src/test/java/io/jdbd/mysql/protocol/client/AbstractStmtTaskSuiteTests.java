package io.jdbd.mysql.protocol.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.type.City;
import io.jdbd.mysql.type.TrueOrFalse;
import io.jdbd.mysql.util.*;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.GeometryUtils;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.*;
import java.util.*;

import static org.testng.Assert.*;

public abstract class AbstractStmtTaskSuiteTests extends AbstractTaskSuiteTests {


    private final SubType subType;

    protected AbstractStmtTaskSuiteTests(SubType subType) {
        this.subType = subType;
    }

    abstract Mono<ResultStates> executeUpdate(ParamStmt stmt, TaskAdjutant adjutant);

    abstract Flux<ResultRow> executeQuery(ParamStmt stmt, TaskAdjutant adjutant);

    abstract Logger obtainLogger();

    final void doBigIntBindAndExtract(Logger LOG) {
        LOG.info("bigIntBindAndExtract test start");

        final String sql = "SELECT t.id as id, t.create_time as createTime FROM mysql_types as t WHERE t.id  = ?";
        final long id = convertId(2);
        ParamValue bindValue = JdbdValues.paramValue(0, MySQLType.BIGINT, id);

        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();

        List<ResultRow> resultRowList;
        resultRowList = ComQueryTask.paramQuery(Stmts.single(sql, bindValue), DatabaseProtocol.ROW_FUNC, taskAdjutant)
                .collectList()
                .block();

        assertFalse(MySQLCollections.isEmpty(resultRowList), "resultRowList");
        ResultRow resultRow = resultRowList.get(0);
        assertNotNull(resultRow, "resultRow");
        Long resultId = resultRow.get("id", Long.class);

        assertEquals(resultId, Long.valueOf(id), "id");

        // string bigint
        bindValue = JdbdValues.paramValue(0, MySQLType.BIGINT, Long.toString(id));
        resultRowList = ComQueryTask.paramQuery(Stmts.single(sql, bindValue), DatabaseProtocol.ROW_FUNC, taskAdjutant)
                .collectList()
                .block();

        assertFalse(MySQLCollections.isEmpty(resultRowList), "resultRowList");
        resultRow = resultRowList.get(0);
        assertNotNull(resultRow, "resultRow");
        resultId = resultRow.get("id", Long.class);

        assertEquals(resultId, Long.valueOf(id), "id");

        LOG.info("bigIntBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doDateBindAndExtract(Logger LOG) {
        LOG.info("doDateBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();

        assertDateBindAndExtract(taskAdjutant, LocalDate.now());
        assertDateBindAndExtract(taskAdjutant, "2021-04-10");

        LOG.info("doDateBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doTimeBindAndExtract(Logger LOG) {
        LOG.info("doTimeBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String field;

        field = "my_time";

        assertTimeBindAndExtract(taskAdjutant, LocalTime.now(), field);
        assertTimeBindAndExtract(taskAdjutant, OffsetTime.now(), field);
        assertTimeBindAndExtract(taskAdjutant, "19:26:00.999999", field);
        assertTimeBindAndExtract(taskAdjutant, "00:00:00", field);

        assertTimeBindAndExtract(taskAdjutant, "00:00:00.999999", field);
        assertTimeBindAndExtract(taskAdjutant, "23:59:59", field);
        assertTimeBindAndExtract(taskAdjutant, "23:59:59.999999", field);

        assertTimeBindAndExtract(taskAdjutant, Duration.ZERO, field);
        assertTimeBindAndExtract(taskAdjutant, MySQLTimes.parseTimeAsDuration("838:59:59"), field);
        assertTimeBindAndExtract(taskAdjutant, MySQLTimes.parseTimeAsDuration("-838:59:59.000000"), field);


        field = "my_time1";

        assertTimeBindAndExtract(taskAdjutant, LocalTime.now(), field);
        assertTimeBindAndExtract(taskAdjutant, OffsetTime.now(), field);
        assertTimeBindAndExtract(taskAdjutant, "19:26:00.999999", field);
        assertTimeBindAndExtract(taskAdjutant, "00:00:00", field);

        assertTimeBindAndExtract(taskAdjutant, "00:00:00.999999", field);
        assertTimeBindAndExtract(taskAdjutant, "23:59:59", field);
        assertTimeBindAndExtract(taskAdjutant, "23:59:59.999999", field);

        assertTimeBindAndExtract(taskAdjutant, Duration.ZERO, field);
        assertTimeBindAndExtract(taskAdjutant, MySQLTimes.parseTimeAsDuration("838:59:59"), field);
        assertTimeBindAndExtract(taskAdjutant, MySQLTimes.parseTimeAsDuration("-838:59:59.000000"), field);


        LOG.info("doTimeBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doDatetimeBindAndExtract(Logger LOG) {
        LOG.info("datetimeBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();

        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, "2021-03-16 19:26:00.999999", "create_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, LocalDateTime.now(), "create_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, OffsetDateTime.now(ZoneOffset.of("+03:00")), "create_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, ZonedDateTime.now(ZoneOffset.of("+04:00")), "create_time");

        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, "2021-03-16 19:26:00.999999", "update_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, LocalDateTime.now(), "update_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, OffsetDateTime.now(ZoneOffset.of("+03:00")), "update_time");
        assertDateTimeModify(taskAdjutant, MySQLType.DATETIME, ZonedDateTime.now(ZoneOffset.of("+04:00")), "update_time");

        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, "2021-03-16 19:26:00.999999", "my_timestamp");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, LocalDateTime.parse("2021-03-16 19:26:00.999999", MySQLTimes.MYSQL_DATETIME_FORMATTER), "my_timestamp");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, OffsetDateTime.parse("2021-03-16 19:26:00.999999+03:00", MySQLTimes.MYSQL_DATETIME_OFFSET_FORMATTER), "my_timestamp");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, ZonedDateTime.parse("2021-03-16 19:26:00.999999+04:00", MySQLTimes.MYSQL_DATETIME_OFFSET_FORMATTER), "my_timestamp");

        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, "2021-03-16 19:26:00.999999", "my_timestamp1");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, LocalDateTime.parse("2021-03-16 19:26:00.999999", MySQLTimes.MYSQL_DATETIME_FORMATTER), "my_timestamp1");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, OffsetDateTime.parse("2021-03-16 19:26:00.999999+03:00", MySQLTimes.MYSQL_DATETIME_OFFSET_FORMATTER), "my_timestamp1");
        assertDateTimeModify(taskAdjutant, MySQLType.TIMESTAMP, ZonedDateTime.parse("2021-03-16 19:26:00.999999+04:00", MySQLTimes.MYSQL_DATETIME_OFFSET_FORMATTER), "my_timestamp1");

        LOG.info("datetimeBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doStringBindAndExtract(Logger LOG) {
        LOG.info("stringBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();

        String bindParma = "  \"\\'秦军\\'\\'\\'\\'\\'\\'\"\\Z\n\r update mysql_types as t set t.name = 'evil' where t.id = 5 %% " + '\032';

        assertStringBindAndExtract(taskAdjutant, MySQLType.VARCHAR, bindParma, "name");
        assertStringBindAndExtract(taskAdjutant, MySQLType.CHAR, bindParma, "my_char");

        bindParma = "  'evil' , t.my_decimal = 999999.00   ";
        assertStringBindAndExtract(taskAdjutant, MySQLType.VARCHAR, bindParma, "name");
        assertStringBindAndExtract(taskAdjutant, MySQLType.CHAR, bindParma, "my_char");

        assertStringBindAndExtract(taskAdjutant, MySQLType.CHAR, "             ", "my_char");


        LOG.info("stringBindAndExtract test success");
        releaseConnection(taskAdjutant);

    }

    final void doBinaryBindAndExtract(Logger LOG) {
        LOG.info("binaryBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();

        String bindParam = "'china' %_#\\'\\' \" '秦军' '中国' \00   ";
        assertBinaryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, bindParam, "my_var_binary");
        assertBinaryBindAndExtract(taskAdjutant, MySQLType.BINARY, bindParam, "my_binary");

        bindParam = "  'evil' , t.my_decimal = 999999.00   ";
        assertBinaryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, bindParam, "my_var_binary");
        assertBinaryBindAndExtract(taskAdjutant, MySQLType.BINARY, bindParam, "my_binary");

        LOG.info("binaryBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doBitBindAndExtract(Logger LOG) {
        LOG.info("bitBindAndExtract test start");
        final TaskAdjutant adjutant = obtainTaskAdjutant();
        String field;
        field = "my_bit";

        assertBitBindAndExtract(adjutant, -1L, field);
        assertBitBindAndExtract(adjutant, Long.MAX_VALUE, field);
        assertBitBindAndExtract(adjutant, Long.MIN_VALUE, field);
        assertBitBindAndExtract(adjutant, 0L, field);

        assertBitBindAndExtract(adjutant, -1, field);
        assertBitBindAndExtract(adjutant, Integer.MAX_VALUE, field);
        assertBitBindAndExtract(adjutant, Integer.MIN_VALUE, field);
        assertBitBindAndExtract(adjutant, 0, field);

        assertBitBindAndExtract(adjutant, (short) -1, field);
        assertBitBindAndExtract(adjutant, Short.MAX_VALUE, field);
        assertBitBindAndExtract(adjutant, Short.MIN_VALUE, field);
        assertBitBindAndExtract(adjutant, (short) 0, field);

        assertBitBindAndExtract(adjutant, (byte) -1, field);
        assertBitBindAndExtract(adjutant, Byte.MAX_VALUE, field);
        assertBitBindAndExtract(adjutant, Byte.MIN_VALUE, field);
        assertBitBindAndExtract(adjutant, (byte) 0, field);

        assertBitBindAndExtract(adjutant, "秦军", field);
        assertBitBindAndExtract(adjutant, MySQLNumbers.longToBigEndianBytes(-1L), field);
        assertBitBindAndExtract(adjutant, MySQLNumbers.longToBigEndianBytes(0L), field);


        field = "my_bit20";
        assertBitBindAndExtract(adjutant, Long.parseLong("10101101001101", 2), field);
        assertBitBindAndExtract(adjutant, 0B1111_1111_1111_1111_1111L, field);
        assertBitBindAndExtract(adjutant, 0L, field);

        assertBitBindAndExtract(adjutant, MySQLNumbers.longToBigEndianBytes(0B0000_0000_0000_0000L), field);

        LOG.info("bitBindAndExtract test success");
        releaseConnection(adjutant);
    }


    final void doTinyint1BindExtract(Logger LOG) {
        throw new UnsupportedOperationException();

    }

    final void doNumberBindAndExtract(Logger LOG) {
        LOG.info("numberBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String field = "my_tinyint";
        MySQLType mySQLType = MySQLType.TINYINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, (byte) 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, (byte) -1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Byte.MAX_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Byte.MIN_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_tinyint_unsigned";
        mySQLType = MySQLType.TINYINT_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) 1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFF, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_smallint";
        mySQLType = MySQLType.SMALLINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, (short) -1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Short.MAX_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Short.MIN_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_smallint_unsigned";
        mySQLType = MySQLType.SMALLINT_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFFFF, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_mediumint";
        mySQLType = MySQLType.MEDIUMINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -0x7FFF_FF, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0x7FFF_FF, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_mediumint_unsigned";
        mySQLType = MySQLType.MEDIUMINT_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFFFF_FF, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_int";
        mySQLType = MySQLType.INT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Integer.MIN_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Integer.MAX_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_int_unsigned";
        mySQLType = MySQLType.INT_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0xFFFF_FFFFL, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_bigint";
        mySQLType = MySQLType.BIGINT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Long.MIN_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, Long.MAX_VALUE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_bigint_unsigned";
        mySQLType = MySQLType.BIGINT_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, MySQLNumbers.MAX_UNSIGNED_LONG, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_decimal";
        mySQLType = MySQLType.DECIMAL;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("34234234.09"), field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("-34234234.09"), field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);

        field = "my_decimal_unsigned";
        mySQLType = MySQLType.DECIMAL_UNSIGNED;
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1L, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("34234234.09"), field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, new BigDecimal("34234234.1"), field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigInteger.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);


        field = "my_float";
        mySQLType = MySQLType.FLOAT;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0F, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1.0F, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);

        field = "my_float_unsigned";
        mySQLType = MySQLType.FLOAT_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0F, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1.0F, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "1", field);

        field = "my_double";
        mySQLType = MySQLType.DOUBLE;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0D, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, -1.0F, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "-1", field);

        field = "my_double_unsigned";
        mySQLType = MySQLType.DOUBLE_UNSIGNED;

        assertNumberBindAndExtract(taskAdjutant, mySQLType, 0.0D, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, 1.0D, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, BigDecimal.ONE, field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "0", field);
        assertNumberBindAndExtract(taskAdjutant, mySQLType, "1", field);


        LOG.info("numberBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doEnumBindAndExtract(Logger LOG) {
        LOG.info("enumBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();

        assertEnumBindExtract(taskAdjutant, MySQLType.CHAR, TrueOrFalse.T.name());
        assertEnumBindExtract(taskAdjutant, MySQLType.CHAR, TrueOrFalse.F.name());
        assertEnumBindExtract(taskAdjutant, MySQLType.ENUM, TrueOrFalse.T.name());
        assertEnumBindExtract(taskAdjutant, MySQLType.ENUM, TrueOrFalse.F.name());

        assertEnumBindExtract(taskAdjutant, MySQLType.ENUM, TrueOrFalse.T);
        assertEnumBindExtract(taskAdjutant, MySQLType.ENUM, TrueOrFalse.F);

        LOG.info("enumBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doSetTypeBindAndExtract(Logger LOG) {
        LOG.info("setTypeBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();

        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.CHAR, City.BEIJING.name());
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.CHAR, City.AOMENG.name());
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.CHAR, City.SHANGHAI);
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.SET, MySQLArrays.asUnmodifiableSet(City.BEIJING.name(), City.SHANGHAI.name(), City.SHENZHEN.name(), City.TAIBEI.name()));

        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.SET, EnumSet.of(City.BEIJING, City.SHANGHAI, City.SHENZHEN, City.TAIBEI));
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.SET, City.AOMENG);
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.SET, "AOMENG,SHANGHAI");
        assertSetTypeBindAndExtract(taskAdjutant, MySQLType.SET, "AOMENG");

        LOG.info("setTypeBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doJsonBindAndExtract(Logger LOG) throws Exception {
        LOG.info("jsonBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();


        Map<String, Object> map = new HashMap<>();
        map.put("id", 1L);
        map.put("name", "''''\"\",\\,_%\032     ");

        final String id = "10", field = "my_json";
        final ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(map);
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.JSON, json, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);
        final String resultJson = resultRow.get("field", String.class);

        final JsonNode root = mapper.readTree(resultJson);
        assertEquals(root.at("/id").asLong(), map.get("id"));
        assertEquals(root.at("/name").asText(), map.get("name"), field);

        LOG.info("jsonBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doTinyBlobBindExtract(Logger LOG) {
        LOG.info("doTinyBlobBindExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.charsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertTinyBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTinyBlobBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertTinyBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTinyBlobBindAndExtract(taskAdjutant, array);

        LOG.info("doTinyBlobBindExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doBlobBindExtract(Logger LOG) {
        LOG.info("doBlobBindExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.charsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertBlobBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertBlobBindAndExtract(taskAdjutant, array);

        LOG.info("doBlobBindExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doMediumBlobBindExtract(Logger LOG) {
        LOG.info("doMediumBlobBindExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.charsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertMediumBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertMediumBlobBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertMediumBlobBindAndExtract(taskAdjutant, text);

        array = text.getBytes(charset);
        assertMediumBlobBindAndExtract(taskAdjutant, array);

        LOG.info("doMediumBlobBindExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doLongBlobBindExtract(Logger LOG) {
        LOG.info("doLongBlobBindExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.charsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertLongBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertLongBlobBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertLongBlobBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertLongBlobBindAndExtract(taskAdjutant, array);

        LOG.info("doLongBlobBindExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doTinyTextBindAndExtract(Logger LOG) {
        LOG.info("doTinyTextBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.charsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertTinyTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTinyTextBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertTinyTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTinyTextBindAndExtract(taskAdjutant, array);

        LOG.info("doTinyTextBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doTextBindAndExtract(Logger LOG) {
        LOG.info("doTextBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.charsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTextBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertTextBindAndExtract(taskAdjutant, array);

        LOG.info("doTextBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doMediumTextBindAndExtract(Logger LOG) {
        LOG.info("doMediumTextBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.charsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertMediumTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertMediumTextBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertMediumTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertMediumTextBindAndExtract(taskAdjutant, array);

        LOG.info("doMediumTextBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doLongTextBindAndExtract(Logger LOG) {
        LOG.info("doLongTextBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        final Charset charset = taskAdjutant.charsetClient();
        String text;
        byte[] array;

        text = "'evil,\"sql inject\"' '\\0' \u001a,set my_decimal = '9999.0'";
        assertLongTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertLongTextBindAndExtract(taskAdjutant, array);

        text = "'''''' \"\"\" \u001a \u001a % _";
        assertLongTextBindAndExtract(taskAdjutant, text);
        array = text.getBytes(charset);
        assertLongTextBindAndExtract(taskAdjutant, array);

        LOG.info("doLongTextBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }

    final void doGeometryBindAndExtract(Logger LOG) {
        LOG.info("geometryBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("POINT(%s %s)", Double.MAX_VALUE, Double.MIN_VALUE);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = GeometryUtils.geometryToWkb(wktText, true);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        wktText = String.format(" LINESTRING (  0 0, 1.0 3.3 ,   %s %s  )", Double.MAX_VALUE, Double.MIN_VALUE);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = GeometryUtils.geometryToWkb(wktText, true);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        wktText = "POLYGON((0 0,0 1,0 3,0 0))";
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = GeometryUtils.geometryToWkb(wktText, true);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        wktText = " MULTIPOINT ( ( 0 0 ) , (1 1),(1 3), (0 0))  ";
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_POINT with big endian .
//        wkbArray = Geometries.geometryToWkb(wktText, true);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        wktText = String.format(" MULTILINESTRING ( (0.0 1.3 ,3 3),(3.4 34.5 ,%s %s) )"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_LINE_STRING with big endian .
//        wkbArray = Geometries.geometryToWkb(wktText, true);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        wktText = String.format("MULTIPOLYGON ( ((0 0 ,3 4,5 8 , 0 0))  ,((1.3 3.5 ,7 4,5 9 ,%s %s,1.3 3.5)) ) "
                , Double.MAX_VALUE, Double.MIN_VALUE);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_POLYGON with big endian .
//        wkbArray = Geometries.geometryToWkb(wktText, true);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        final String point, lineString, polygon, multiPoint, multiLineString, multiPolygon, geometryCollection;

        point = String.format(" POINT  ( %s %s)  ", Double.MAX_VALUE, Double.MIN_VALUE);
        lineString = String.format(" LINESTRING (  0 0, 1.0 3.3 ,   %s %s  )", Double.MAX_VALUE, Double.MIN_VALUE);
        polygon = String.format("POLYGON((0 0,0 1,0 3,0 0),(3 4,0 1,0 3,%s %s,3 4))"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        multiPoint = " MULTIPOINT ( ( 0 0 ) , (1 1),(1 3), (0 0))  ";
        multiLineString = String.format(" MULTILINESTRING ( (0.0 1.3 ,3 3),(3.4 34.5 ,%s %s) )"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        multiPolygon = String.format("MULTIPOLYGON ( ((0 0 ,3 4,5 8 , 0 0))  ,((1.3 3.5 ,7 4,5 9 ,%s %s,1.3 3.5)) ) "
                , Double.MAX_VALUE, Double.MIN_VALUE);
        geometryCollection = "GEOMETRYCOLLECTION(POINT(0 0))";


        wktText = String.format("GEOMETRYCOLLECTION (%s,%s,%s,%s ,%s,%s,%s) "
                , point
                , lineString
                , polygon
                , multiPoint

                , multiLineString
                , multiPolygon
                , geometryCollection
        );
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse GEOMETRY_COLLECTION with big endian . @see https://bugs.mysql.com/bug.php?id=103262&thanks=4
//        wkbArray = Geometries.geometryToWkb(wktText, true);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.geometryToWkb(wktText, false);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("geometryBindAndExtract test success");
        releaseConnection(taskAdjutant);

    }


    final void doPointBindAndExtract(Logger LOG) {
        LOG.info("pointBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("POINT(%s %s)", Double.MAX_VALUE, Double.MIN_VALUE);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = GeometryUtils.pointToWkb(wktText, true);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.pointToWkb(wktText, false);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertPointsBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);


        LOG.info("pointBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doLineStringBindAndExtract(Logger LOG) {
        LOG.info("lineStringBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format(" LINESTRING (  0 0, 1.0 3.3 ,   %s %s  )", Double.MAX_VALUE, Double.MIN_VALUE);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = GeometryUtils.lineStringToWkb(wktText, true);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.lineStringToWkb(wktText, false);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertLineStringBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("lineStringBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doPolygonBindAndExtract(Logger LOG) {
        LOG.info("polygonBindAndExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("POLYGON((0 0,0 1,0 3,0 0),(3 4,0 1,0 3,%s %s,3 4))"
                , Double.MAX_VALUE, Double.MIN_VALUE);

        assertPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertPolygonBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        wkbArray = GeometryUtils.polygonToWkb(wktText, true);
        assertPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertPolygonBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.polygonToWkb(wktText, false);
        assertPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertPolygonBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);
        LOG.info("polygonBindAndExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doMultiPointBindExtract(Logger LOG) {
        LOG.info("multiPointBindExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("MULTIPOINT(( 0 0 ),(1 1),(1 3), (0 0),(%s %s)) "
                , Double.MIN_VALUE, Double.MAX_VALUE);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_POINT with big endian . @see https://bugs.mysql.com/bug.php?id=103262&thanks=4
//        wkbArray = Geometries.multiPointToWkb(wktText, true);
//        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.multiPointToWkb(wktText, false);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertMultiPointBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("multiPointBindExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doMultiLineStringBindExtract(Logger LOG) {
        LOG.info("multiLineStringBindExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format(" MULTILINESTRING ( (0.0 1.3 ,3 3),(3.4 34.5 ,%s %s) )"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_LINE_STRING with big endian .
//        wkbArray = Geometries.multiLineStringToWkb(wktText, true);
//        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.multiLineStringToWkb(wktText, false);
        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertMultiLineStringBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("multiLineStringBindExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doMultiPolygonBindExtract(Logger LOG) {
        LOG.info("multiPolygonBindExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;

        wktText = String.format("MULTIPOLYGON ( ((0 0 ,3 4,5 8 , 0 0))  ,((1.3 3.5 ,7 4,5 9 ,%s %s,1.3 3.5)) ) "
                , Double.MAX_VALUE, Double.MIN_VALUE);
        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse MULTI_POLYGON with big endian .
//        wkbArray = Geometries.multiPolygonToWkb(wktText, true);
//        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.multiPolygonToWkb(wktText, false);
        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertMultiPolygonBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("multiPolygonBindExtract test success");
        releaseConnection(taskAdjutant);
    }


    final void doGeometryCollectionBindExtract(Logger LOG) {
        LOG.info("geometryCollectionBindExtract test start");
        final TaskAdjutant taskAdjutant = obtainTaskAdjutant();
        String wktText;
        byte[] wkbArray;


        final String point, lineString, polygon, multiPoint, multiLineString, multiPolygon, geometryCollection;

        point = String.format(" POINT  ( %s %s)  ", Double.MAX_VALUE, Double.MIN_VALUE);
        lineString = String.format(" LINESTRING (  0 0, 1.0 3.3 ,   %s %s  )", Double.MAX_VALUE, Double.MIN_VALUE);
        polygon = String.format("POLYGON((0 0,0 1,0 3,0 0),(3 4,0 1,0 3,%s %s,3 4))"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        multiPoint = " MULTIPOINT ( ( 0 0 ) , (1 1),(1 3), (0 0))  ";
        multiLineString = String.format(" MULTILINESTRING ( (0.0 1.3 ,3 3),(3.4 34.5 ,%s %s) )"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        multiPolygon = String.format("MULTIPOLYGON ( ((0 0 ,3 4,5 8 , 0 0))  ,((1.3 3.5 ,7 4,5 9 ,%s %s,1.3 3.5)) ) "
                , Double.MAX_VALUE, Double.MIN_VALUE);
        geometryCollection = "GEOMETRYCOLLECTION(POINT(0 0))";


        wktText = String.format("GEOMETRYCOLLECTION (%s,%s,%s,%s ,%s,%s,%s) "
                , point
                , lineString
                , polygon
                , multiPoint

                , multiLineString
                , multiPolygon
                , geometryCollection
        );
        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wktText);
        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.VARCHAR, wktText);

        //MySQL 8.0.23 bug ,can't parse GEOMETRY_COLLECTION with big endian .
//        wkbArray = Geometries.geometryCollectionToWkb(wktText, true);
//        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
//        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        wkbArray = GeometryUtils.geometryCollectionToWkb(wktText, false);
        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.GEOMETRY, wkbArray);
        assertGeometryCollectionBindAndExtract(taskAdjutant, MySQLType.VARBINARY, wkbArray);

        LOG.info("geometryCollectionBindExtract test success");
        releaseConnection(taskAdjutant);
    }


    /*################################## blow private method ##################################*/

    /**
     * @see #doGeometryBindAndExtract(Logger)
     */
    private void assertGeometryBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(11);
        final String field = "my_geometry";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = GeometryUtils.geometryToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(GeometryUtils.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #doPointBindAndExtract(Logger)
     */
    private void assertPointsBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(12);
        final String field = "my_point";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = GeometryUtils.pointToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(GeometryUtils.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #doLineStringBindAndExtract(Logger)
     */
    private void assertLineStringBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(13);
        final String field = "my_linestring";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = GeometryUtils.lineStringToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(GeometryUtils.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #doPolygonBindAndExtract(Logger)
     */
    private void assertPolygonBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(14);
        final String field = "my_polygon";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = GeometryUtils.polygonToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(GeometryUtils.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #doMultiPointBindExtract(Logger)
     */
    private void assertMultiPointBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(15);
        final String field = "my_multipoint";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = GeometryUtils.multiPointToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(GeometryUtils.wkbEquals(resultWkb, bindWkb), field);
    }


    /**
     * @see #doMultiLineStringBindExtract(Logger)
     */
    private void assertMultiLineStringBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(16);
        final String field = "my_multilinestring";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = GeometryUtils.multiLineStringToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(GeometryUtils.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #doMultiPolygonBindExtract(Logger)
     */
    private void assertMultiPolygonBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(17);
        final String field = "my_multipolygon";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = GeometryUtils.multiPolygonToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(GeometryUtils.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #doGeometryCollectionBindExtract(Logger)
     */
    private void assertGeometryCollectionBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(18);
        final String field = "my_geometrycollection";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] resultWkb = resultRow.getNonNull("field", byte[].class);

        final byte[] bindWkb;
        if (bindParam instanceof String) {
            bindWkb = GeometryUtils.geometryCollectionToWkb((String) bindParam, true);
        } else {
            bindWkb = (byte[]) bindParam;
        }
        assertTrue(GeometryUtils.wkbEquals(resultWkb, bindWkb), field);
    }

    /**
     * @see #doDateBindAndExtract(Logger)
     */
    private void assertDateBindAndExtract(final TaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(19);
        final String field = "my_date";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.DATE, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final LocalDate resultDate = resultRow.getNonNull("field", LocalDate.class);

        final LocalDate bindDate;
        if (bindParam instanceof String) {
            bindDate = LocalDate.parse((String) bindParam);
        } else {
            bindDate = (LocalDate) bindParam;
        }
        assertEquals(resultDate, bindDate, field);
    }

    /**
     * @see #doTimeBindAndExtract(Logger)
     */
    private void assertTimeBindAndExtract(final TaskAdjutant taskAdjutant, final Object bindParam, final String field) {
        final long id = convertId(20);
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.TIME, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);


        if (bindParam instanceof Duration) {
            final Duration resultDuration = resultRow.getNonNull("field", Duration.class);
            assertEquals(resultDuration, bindParam, field);
            return;
        }

        final LocalTime bindTime, resultTime = resultRow.getNonNull("field", LocalTime.class);
        if (bindParam instanceof String) {
            bindTime = LocalTime.parse((String) bindParam, MySQLTimes.MYSQL_TIME_FORMATTER);
        } else if (bindParam instanceof OffsetTime) {
            bindTime = ((OffsetTime) bindParam).withOffsetSameInstant(taskAdjutant.connZone())
                    .toLocalTime();
        } else {
            bindTime = (LocalTime) bindParam;
        }


        final LocalTime time = MySQLTimes.truncatedIfNeed(resultRow.getRowMeta().getScale("field"), bindTime);
        if (taskAdjutant.sessionEnv().containSqlMode(SQLMode.TIME_TRUNCATE_FRACTIONAL)) {
            assertEquals(resultTime, time, field);
        } else {
            Duration duration = Duration.between(time, resultTime);
            assertFalse(duration.isNegative(), field);
            assertTrue(duration.getSeconds() < 1L, field);
        }

    }


    /**
     * @see #doTinyBlobBindExtract(Logger)
     */
    private void assertTinyBlobBindAndExtract(final TaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(21);
        final String field = "my_tinyblob";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.TINYBLOB, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] bindBinary;
        if (bindParam instanceof String) {
            bindBinary = ((String) bindParam).getBytes(taskAdjutant.charsetClient());
        } else {
            bindBinary = (byte[]) bindParam;
        }
        assertEquals(resultRow.getNonNull("field", byte[].class), bindBinary, field);
    }

    /**
     * @see #doBlobBindExtract(Logger)
     */
    private void assertBlobBindAndExtract(final TaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(22);
        final String field = "my_blob";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.BLOB, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] bindBinary;
        if (bindParam instanceof String) {
            bindBinary = ((String) bindParam).getBytes(taskAdjutant.charsetClient());
        } else {
            bindBinary = (byte[]) bindParam;
        }
        assertEquals(resultRow.getNonNull("field", byte[].class), bindBinary, field);
    }

    /**
     * @see #doMediumBlobBindExtract(Logger)
     */
    private void assertMediumBlobBindAndExtract(final TaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(23);
        final String field = "my_medium_blob";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.MEDIUMBLOB, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] bindBinary;
        if (bindParam instanceof String) {
            bindBinary = ((String) bindParam).getBytes(taskAdjutant.charsetClient());
        } else {
            bindBinary = (byte[]) bindParam;
        }
        assertEquals(resultRow.getNonNull("field", byte[].class), bindBinary, field);
    }

    /**
     * @see #doLongBlobBindExtract(Logger)
     */
    private void assertLongBlobBindAndExtract(final TaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(24);
        final String field = "my_long_blob";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.LONGBLOB, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final byte[] bindBinary;
        if (bindParam instanceof String) {
            bindBinary = ((String) bindParam).getBytes(taskAdjutant.charsetClient());
        } else {
            bindBinary = (byte[]) bindParam;
        }
        assertEquals(resultRow.getNonNull("field", byte[].class), bindBinary, field);
    }

    /**
     * @see #doTinyTextBindAndExtract(Logger)
     */
    private void assertTinyTextBindAndExtract(final TaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(25);
        final String field = "my_tiny_text";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.TINYTEXT, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final String bindText;
        if (bindParam instanceof String) {
            bindText = ((String) bindParam);
        } else {
            bindText = new String((byte[]) bindParam, taskAdjutant.charsetClient());
        }
        assertEquals(resultRow.getNonNull("field", String.class), bindText, field);
    }

    /**
     * @see #doTextBindAndExtract(Logger)
     */
    private void assertTextBindAndExtract(final TaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(26);
        final String field = "my_text";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.TEXT, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final String bindText;
        if (bindParam instanceof String) {
            bindText = ((String) bindParam);
        } else {
            bindText = new String((byte[]) bindParam, taskAdjutant.charsetClient());
        }
        assertEquals(resultRow.getNonNull("field", String.class), bindText, field);
    }

    /**
     * @see #doMediumTextBindAndExtract(Logger)
     */
    private void assertMediumTextBindAndExtract(final TaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(27);
        final String field = "my_medium_text";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.MEDIUMTEXT, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final String bindText;
        if (bindParam instanceof String) {
            bindText = ((String) bindParam);
        } else {
            bindText = new String((byte[]) bindParam, taskAdjutant.charsetClient());
        }
        assertEquals(resultRow.getNonNull("field", String.class), bindText, field);
    }

    /**
     * @see #doLongTextBindAndExtract(Logger)
     */
    private void assertLongTextBindAndExtract(final TaskAdjutant taskAdjutant, final Object bindParam) {
        final long id = convertId(28);
        final String field = "my_long_text";
        //1. update filed
        updateSingleField(taskAdjutant, MySQLType.LONGTEXT, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final String bindText;
        if (bindParam instanceof String) {
            bindText = ((String) bindParam);
        } else {
            bindText = new String((byte[]) bindParam, taskAdjutant.charsetClient());
        }
        assertEquals(resultRow.getNonNull("field", String.class), bindText, field);
    }


    /**
     * @see #assertSetTypeBindAndExtract(TaskAdjutant, MySQLType, Object)
     */
    private void assertSetTypeBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType,
                                             final Object bindParam) {
        final long id = convertId(9);
        final String field = "my_set";

        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);
        final Set<City> citySet = resultRow.getSet("field", City.class);
        assertNotNull(citySet, field);

        final Set<City> bindSet;
        if (bindParam instanceof String) {
            bindSet = MySQLStrings.spitAsEnumSet((String) bindParam, ",", City.class);
        } else if (bindParam instanceof City) {
            bindSet = Collections.singleton((City) bindParam);
        } else if (bindParam instanceof Set) {
            Set<?> paramSet = (Set<?>) bindParam;
            Set<City> tempSet = new HashSet<>((int) (paramSet.size() / 0.75F));
            for (Object s : paramSet) {
                if (s instanceof String) {
                    tempSet.add(City.valueOf((String) s));
                } else if (s instanceof City) {
                    tempSet.add((City) s);
                }
            }
            bindSet = Collections.unmodifiableSet(tempSet);
        } else {
            throw new IllegalArgumentException("bindParam type error");
        }

        assertEquals(citySet, bindSet, field);

    }

    /**
     * @see #doEnumBindAndExtract(Logger)
     */
    private void assertEnumBindExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(8);
        final String field = "my_enum";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);
        final TrueOrFalse trueOrFalse = resultRow.get("field", TrueOrFalse.class);
        assertNotNull(trueOrFalse, field);
        if (bindParam instanceof String) {
            assertEquals(trueOrFalse, TrueOrFalse.valueOf((String) bindParam), field);
        } else {
            assertEquals(trueOrFalse, bindParam, field);
        }
    }

    /**
     * @see #doNumberBindAndExtract(Logger)
     */
    private void assertNumberBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType,
                                            final Object bindParam, final String field) {
        final long id = convertId(7);
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final MySQLType fieldType = (MySQLType) resultRow.getRowMeta().getDataType("field");
        final Number resultValue = (Number) resultRow.get("field", fieldType.firstJavaType());
        assertNotNull(resultValue, field);
        if (resultValue instanceof BigDecimal) {
            if (bindParam instanceof Float) {
                throw new IllegalArgumentException("bindParam type error");
            }
            final BigDecimal bindValue;
            if (bindParam instanceof String) {
                bindValue = new BigDecimal((String) bindParam);
            } else if (mySQLType == MySQLType.DOUBLE_UNSIGNED && bindParam instanceof Double) {
                bindValue = BigDecimal.valueOf((Double) bindParam);
            } else {
                bindValue = MySQLNumbers.convertNumberToBigDecimal((Number) bindParam);
            }
            assertEquals(((BigDecimal) resultValue).compareTo(bindValue), 0, field);
        } else if (resultValue instanceof BigInteger) {
            if (bindParam instanceof Double || bindParam instanceof Float) {
                throw new IllegalArgumentException("bindParam type error");
            }
            if (bindParam instanceof String) {
                assertEquals(resultValue, new BigInteger((String) bindParam), field);
            } else {
                assertEquals(resultValue, MySQLNumbers.convertNumberToBigInteger((Number) bindParam), field);
            }
        } else if (resultValue instanceof Double) {
            final double bindValue;
            if (bindParam instanceof String) {
                bindValue = Double.parseDouble((String) bindParam);
            } else if (bindParam instanceof Double || bindParam instanceof Float) {
                bindValue = ((Number) bindParam).doubleValue();
            } else if (bindParam instanceof Number) {
                bindValue = ((Number) bindParam).doubleValue();
            } else {
                throw new IllegalArgumentException("bindParam type error");
            }
            assertEquals(resultValue.doubleValue(), bindValue, field);
        } else if (resultValue instanceof Float) {
            final float bindValue;
            if (bindParam instanceof String) {
                bindValue = Float.parseFloat((String) bindParam);
            } else if (bindParam instanceof Float) {
                bindValue = ((Number) bindParam).floatValue();
            } else if (bindParam instanceof Number) {
                bindValue = ((Number) bindParam).floatValue();
            } else {
                throw new IllegalArgumentException("bindParam type error");
            }
            assertEquals(resultValue.floatValue(), bindValue, field);
        } else {
            long bindValue;
            if (bindParam instanceof String) {
                bindValue = Long.parseLong((String) bindParam);
            } else {
                bindValue = MySQLNumbers.convertNumberToLong((Number) bindParam);
            }
            assertEquals(resultValue.longValue(), bindValue, field);
        }

    }

    /**
     * @see #doTinyint1BindExtract(Logger)
     */
    private void assertTinyInt1BindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam) {
        final long id = convertId(6);
        final String field = "my_tinyint1";
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        if (bindParam instanceof Boolean) {
            assertEquals(resultRow.get("field", Boolean.class), bindParam, field);
        } else if (bindParam instanceof String) {
            Boolean bindValue = MySQLConvertUtils.tryConvertToBoolean((String) bindParam);
            assertEquals(resultRow.get("field", Boolean.class), bindValue, field);
        } else {
            Long resultValue = resultRow.get("field", Long.class);
            assertNotNull(resultValue, field);
            assertEquals(resultValue.longValue(), ((Number) bindParam).longValue(), field);
        }

    }

    /**
     * @see #doBitBindAndExtract(Logger)
     */
    private void assertBitBindAndExtract(final TaskAdjutant adjutant, final Object bindParam
            , final String field) {
        final long id = convertId(5);
        //1. update filed
        updateSingleField(adjutant, MySQLType.BIT, bindParam, field, id);
        //2. query filed
        final ResultRow row;
        row = querySingleField(adjutant, field, id);

        assertEquals(row.getRowMeta().getDataType("field"), MySQLType.BIT, field);

        final Object result = row.get("field");
        assertNotNull(result, field);
        assertEquals(result.getClass(), MySQLType.BIT.firstJavaType(), field);

        final long resultBit = (Long) result;

        final long bindBits;
        if (bindParam instanceof Long
                || bindParam instanceof Integer
                || bindParam instanceof Short
                || bindParam instanceof Byte) {
            // because MySQL server do a widening primitive conversion
            bindBits = ((Number) bindParam).longValue();
        } else if (bindParam instanceof String) {
            // because MySQL server store binary
            byte[] bytes = ((String) bindParam).getBytes(adjutant.charsetClient());
            bindBits = MySQLNumbers.readLongFromBigEndian(bytes, 0, bytes.length);
        } else if (bindParam instanceof byte[]) {
            byte[] bytes = (byte[]) bindParam;
            bindBits = MySQLNumbers.readLongFromBigEndian(bytes, 0, bytes.length);
        } else if (bindParam instanceof BigInteger) {
            bindBits = ((BigInteger) bindParam).longValueExact();
        } else if (bindParam instanceof Double
                || bindParam instanceof Float) {
            // because MySQL server do  round
            bindBits = Math.round(((Number) bindParam).doubleValue());
        } else if (bindParam instanceof BigDecimal) {
            bindBits = ((BigDecimal) bindParam).longValueExact();
        } else {
            // never here
            throw new IllegalArgumentException("bindParam error");
        }
        assertEquals(resultBit, bindBits, field);

    }

    /**
     * @see #doStringBindAndExtract(Logger)
     */
    private void assertBinaryBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final String bindParam, final String field) {
        final long id = convertId(4);
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);
        // 3. compare
        final byte[] bytes = resultRow.get("field", byte[].class);
        assertNotNull(bytes, field);
        final byte[] bindByteArray = bindParam.getBytes(taskAdjutant.charsetClient());
        if (mySQLType == MySQLType.BINARY) {
            final int length = Math.min(bindByteArray.length, bytes.length);
            for (int i = 0; i < length; i++) {
                if (bytes[i] != bindByteArray[i]) {
                    fail("binary type assert failure");
                }
            }
        } else {
            assertTrue(Arrays.equals(bytes, bindByteArray), field);
        }


    }


    /**
     * @see #doStringBindAndExtract(Logger)
     */
    private void assertStringBindAndExtract(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final String bindParam, final String field) {
        final long id = convertId(3);

        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);

        final String string = resultRow.get("field", String.class);
        assertNotNull(string, field);
        if (mySQLType == MySQLType.CHAR) {
            final String actualBindParam = MySQLStrings.trimTrailingSpace(bindParam);
            if (taskAdjutant.sessionEnv().containSqlMode(SQLMode.PAD_CHAR_TO_FULL_LENGTH)) {
                assertTrue(string.startsWith(actualBindParam), field);
                final String tailingSpaces = string.substring(actualBindParam.length());
                assertFalse(MySQLStrings.hasText(tailingSpaces), "tailingSpaces");
            } else {
                assertEquals(string, actualBindParam, field);
            }
        } else {
            assertEquals(string, bindParam, field);
        }


    }


    /**
     * @see #doDatetimeBindAndExtract(Logger)
     */
    private void assertDateTimeModify(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam, final String field) {
        final long id = convertId(2);
        //1. update filed
        updateSingleField(taskAdjutant, mySQLType, bindParam, field, id);
        //2. query filed
        final ResultRow resultRow;
        resultRow = querySingleField(taskAdjutant, field, id);
        final int scale = resultRow.getRowMeta().getScale("field");


        final LocalDateTime resultDateTime = resultRow.get("field", LocalDateTime.class);
        assertNotNull(resultDateTime, field);

        final LocalDateTime bindDateTime;

        if (bindParam instanceof LocalDateTime) {
            bindDateTime = (LocalDateTime) bindParam;
        } else if (bindParam instanceof String) {
            bindDateTime = LocalDateTime.parse((String) bindParam, MySQLTimes.MYSQL_DATETIME_FORMATTER);
        } else if (bindParam instanceof OffsetDateTime) {
            bindDateTime = ((OffsetDateTime) bindParam)
                    .withOffsetSameInstant(taskAdjutant.connZone())
                    .toLocalDateTime();
        } else if (bindParam instanceof ZonedDateTime) {
            bindDateTime = ((ZonedDateTime) bindParam)
                    .withZoneSameInstant(taskAdjutant.connZone())
                    .toLocalDateTime();
        } else {
            // never here
            throw new IllegalArgumentException("bindDatetime type error");
        }


        if (taskAdjutant.sessionEnv().containSqlMode(SQLMode.TIME_TRUNCATE_FRACTIONAL)) {
            assertEquals(MySQLTimes.truncatedIfNeed(scale, bindDateTime), resultDateTime, field);
        } else {
            Duration duration = Duration.between(bindDateTime, resultDateTime);
            if (duration.isNegative() || duration.getSeconds() > 1L) {
                fail(String.format("result datetime[%s] and bind datetime[%s] not match.", resultDateTime, bindDateTime));
            }
        }


    }


    private void updateSingleField(final TaskAdjutant taskAdjutant, final MySQLType mySQLType
            , final Object bindParam, final String field, final Object id) {

        final String paramExp;
        switch (field) {
            case "my_geometry":
            case "my_point":
            case "my_polygon":
            case "my_linestring":
            case "my_multipoint":
            case "my_multilinestring":
            case "my_multipolygon":
            case "my_geometrycollection":
                if (bindParam instanceof String) {
                    paramExp = "ST_GEOMETRYFROMTEXT(?)";
                } else {
                    paramExp = "ST_GEOMETRYFROMWKB(?)";
                }
                break;
            default:
                paramExp = "?";
        }
        String sql = String.format("UPDATE mysql_types as t SET t.%s = %s WHERE t.id = ?", field, paramExp);

        List<ParamValue> bindGroup = new ArrayList<>(2);

        ParamValue bindValue = JdbdValues.paramValue(0, mySQLType, bindParam);

        bindGroup.add(bindValue);
        bindValue = JdbdValues.paramValue(1, MySQLType.BIGINT, id);
        bindGroup.add(bindValue);

        ResultStates resultStates;
        resultStates = executeUpdate(Stmts.paramStmt(sql, bindGroup), taskAdjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.affectedRows(), 1L, "getAffectedRows");
    }

    private ResultRow querySingleField(final TaskAdjutant taskAdjutant, final String field, final Object id) {
        String sql = String.format("SELECT t.id as id, t.%s as field FROM mysql_types as t WHERE t.id = ?", field);
        ParamValue bindValue = JdbdValues.paramValue(0, MySQLType.BIGINT, id);

        List<ResultRow> resultRowList;
        resultRowList = executeQuery(Stmts.single(sql, bindValue), taskAdjutant)
                .collectList()
                .block();
        assertNotNull(resultRowList, "resultRowList");
        assertEquals(resultRowList.size(), 1L, "resultRowList size");
        ResultRow resultRow = resultRowList.get(0);
        assertNotNull(resultRow, "resultRow");
        return resultRow;
    }


    private long convertId(final long id) {
        final long newId;
        switch (this.subType) {
            case COM_QUERY:
                newId = id;
                break;
            case COM_PREPARE_STMT:
                newId = id + 50L;
                break;
            case PREPARE:
                newId = id + 100L;
                break;
            case BINDABLE:
                newId = id + 150L;
                break;
            default:
                throw MySQLExceptions.unexpectedEnum(this.subType);

        }
        return newId;
    }


    protected enum SubType {
        COM_QUERY,
        COM_PREPARE_STMT,
        BINDABLE,
        PREPARE

    }


}
