package io.jdbd.mysql.statement;

import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.statement.BindSingleStatement;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.ZoneOffset;
import java.util.function.Function;

/**
 * <p>
 * This class is responsible for test data type bind and get.
 * </p>
 * <p>
 * All test method's session parameter is closed by {@link #closeSessionAfterTest(Method, ITestContext)}
 * </p>
 */
public class DataTypeTests extends SessionTestSupport {


    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     *     bind {@link MySQLType#BOOLEAN}
     * </p>
     *
     * @see JdbdType#BOOLEAN
     * @see MySQLType#BOOLEAN
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/numeric-type-syntax.html">BOOLEAN </a>
     */
    @Test(invocationCount = 3, dataProvider = "booleanStmtProvider")
    public void booleanType(final BindSingleStatement insertStmt, final BindSingleStatement queryInsert) {
        insertStmt.bind(0, JdbdType.BOOLEAN, null)
                .bind(1, JdbdType.BOOLEAN, true)
                .bind(2, JdbdType.BOOLEAN, false);

        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            // LOG.info("row id : {}", row.get(0, Long.class));
            switch ((int) row.rowNumber()) {
                case 1:
                    Assert.assertNull(row.get(1, Boolean.class));
                    break;
                case 2:
                    Assert.assertEquals(row.get(1, Boolean.class), Boolean.TRUE);
                    break;
                case 3:
                    Assert.assertEquals(row.get(1, Boolean.class), Boolean.FALSE);
                    break;
                default:
                    throw new RuntimeException("unknown row");

            }
            return row.asResultRow();
        };


        Mono.from(insertStmt.executeUpdate())
                .flatMapMany(s -> {
                    // LOG.info("affectedRows : {} , lastId : {}", s.affectedRows(), s.lastInsertedId());
                    final int rowCount = (int) s.affectedRows();
                    long lastId = s.lastInsertedId();
                    for (int i = 0; i < rowCount; i++) {
                        queryInsert.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    return Flux.from(queryInsert.executeQuery(function));
                })
                .blockLast();
    }

    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     *     bind {@link MySQLType#YEAR}
     * </p>
     *
     * @see JdbdType#YEAR
     * @see MySQLType#YEAR
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/year.html">year </a>
     */
    @Test(invocationCount = 3, dataProvider = "yearStmtProvider")
    public void yearType(final BindSingleStatement insertStmt, final BindSingleStatement queryInsert) {
        insertStmt.bind(0, JdbdType.YEAR, null)
                .bind(1, JdbdType.YEAR, Year.of(1901))
                .bind(2, JdbdType.YEAR, Year.of(2155));

        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            // LOG.info("row id : {}", row.get(0, Long.class));
            switch ((int) row.rowNumber()) {
                case 1:
                    Assert.assertNull(row.get(1, Year.class));
                    break;
                case 2:
                    Assert.assertEquals(row.get(1, Year.class), Year.of(1901));
                    break;
                case 3:
                    Assert.assertEquals(row.get(1, Year.class), Year.of(2155));
                    break;
                default:
                    throw new RuntimeException("unknown row");

            }
            return row.asResultRow();
        };


        Mono.from(insertStmt.executeUpdate())
                .flatMapMany(s -> {
                    // LOG.info("affectedRows : {} , lastId : {}", s.affectedRows(), s.lastInsertedId());
                    final int rowCount = (int) s.affectedRows();
                    long lastId = s.lastInsertedId();
                    for (int i = 0; i < rowCount; i++) {
                        queryInsert.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    return Flux.from(queryInsert.executeQuery(function));
                })
                .blockLast();
    }

    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     *     bind {@link MySQLType#DATETIME}
     * </p>
     *
     * @see JdbdType#TIMESTAMP
     * @see JdbdType#TIMESTAMP_WITH_TIMEZONE
     * @see MySQLType#DATETIME
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/datetime.html">DATETIME </a>
     */
    @Test(invocationCount = 3, dataProvider = "datetimeStmtProvider")
    public void datetimeType(final BindSingleStatement insertStmt, final BindSingleStatement queryInsert) {
        final LocalDateTime dateTime, dateTime6, dateTimeFull;
        final OffsetDateTime offsetDateTime, offsetDateTime6, offsetDateTimeFull;

        dateTime = MySQLTimes.truncatedIfNeed(0, LocalDateTime.now());
        dateTime6 = MySQLTimes.truncatedIfNeed(6, LocalDateTime.now());

        offsetDateTime = MySQLTimes.truncatedIfNeed(0, OffsetDateTime.now(ZoneOffset.UTC));
        offsetDateTime6 = MySQLTimes.truncatedIfNeed(6, OffsetDateTime.now(ZoneOffset.UTC));

        dateTimeFull = MySQLTimes.truncatedIfNeed(0, LocalDateTime.now());
        offsetDateTimeFull = MySQLTimes.truncatedIfNeed(6, OffsetDateTime.parse("2023-09-01 09:40:26.999999+08:00", MySQLTimes.OFFSET_DATETIME_FORMATTER_6));

        insertStmt.bind(0, JdbdType.TIMESTAMP, null)
                .bind(1, JdbdType.TIMESTAMP, null)

                .bind(2, JdbdType.TIMESTAMP, dateTime)
                .bind(3, JdbdType.TIMESTAMP, dateTime6)

                .bind(4, JdbdType.TIMESTAMP, offsetDateTime)
                .bind(5, JdbdType.TIMESTAMP, offsetDateTime6)

                .bind(6, JdbdType.TIMESTAMP, dateTimeFull)
                .bind(7, JdbdType.TIMESTAMP, offsetDateTimeFull);

        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            // LOG.info("row id : {}", row.get(0, Long.class));
            switch ((int) row.rowNumber()) {
                case 1:
                    Assert.assertNull(row.get(1, LocalDateTime.class));
                    Assert.assertNull(row.get(2, LocalDateTime.class));
                    break;
                case 2:
                    Assert.assertTrue(dateTime.isEqual(row.getNonNull(1, LocalDateTime.class)));
                    Assert.assertTrue(dateTime6.isEqual(row.getNonNull(2, LocalDateTime.class)));
                    break;
                case 3:
                    Assert.assertTrue(offsetDateTime.isEqual(row.getNonNull(1, OffsetDateTime.class)));
                    Assert.assertTrue(offsetDateTime6.isEqual(row.getNonNull(2, OffsetDateTime.class)));
                    break;
                case 4:
                    Assert.assertTrue(dateTimeFull.isEqual(row.getNonNull(1, LocalDateTime.class)));
                    Assert.assertTrue(offsetDateTimeFull.isEqual(row.getNonNull(2, OffsetDateTime.class)));
                    break;
                default:
                    throw new RuntimeException("unknown row");

            }
            return row.asResultRow();
        };


        Mono.from(insertStmt.executeUpdate())
                .flatMapMany(s -> {
                    // LOG.info("affectedRows : {} , lastId : {}", s.affectedRows(), s.lastInsertedId());
                    final int rowCount = (int) s.affectedRows();
                    long lastId = s.lastInsertedId();
                    for (int i = 0; i < rowCount; i++) {
                        queryInsert.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    return Flux.from(queryInsert.executeQuery(function));
                })
                .blockLast();
    }

    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     *     bind {@link MySQLType#TEXT} , {@link MySQLType#BLOB}
     * </p>
     *
     * @see JdbdType#TEXT
     * @see JdbdType#BLOB
     * @see MySQLType#TEXT
     * @see MySQLType#BLOB
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/string-type-syntax.html">TEXT</a>
     */
    @Test(invocationCount = 3, dataProvider = "textStmtProvider")
    public void textType(final BindSingleStatement insertStmt, final BindSingleStatement queryInsert) {
        final String text1, text2, text3;
        final byte[] blob1, blob2, blob3;

        text1 = "QinArmy's \n \\ \t \" \032 \b \r  ";
        text2 = "QinArmy(秦军) is a open source organization that create better framework,better language";
        text3 = "' update mysql_types AS t SET t.my_decimal = t.my_decimal + 8888.88 WHERE";

        blob1 = text1.getBytes(StandardCharsets.UTF_8);
        blob2 = text2.getBytes(StandardCharsets.UTF_8);
        blob3 = text3.getBytes(StandardCharsets.UTF_8);

        insertStmt.bind(0, JdbdType.TEXT, null)
                .bind(1, JdbdType.BLOB, null)

                .bind(2, JdbdType.TEXT, text1)
                .bind(3, JdbdType.BLOB, blob1)

                .bind(4, JdbdType.TEXT, text2)
                .bind(5, JdbdType.BLOB, blob2)

                .bind(6, JdbdType.TEXT, text3)
                .bind(7, JdbdType.BLOB, blob3);

        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            // LOG.info("row id : {}", row.get(0, Long.class));
            switch ((int) row.rowNumber()) {
                case 1:
                    Assert.assertNull(row.get(1, LocalDateTime.class));
                    Assert.assertNull(row.get(2, LocalDateTime.class));
                    break;
                case 2:
                    Assert.assertEquals(row.get(1, String.class), text1);
                    Assert.assertEquals(row.get(2, byte[].class), blob1);
                    break;
                case 3:
                    Assert.assertEquals(row.get(1, String.class), text2);
                    Assert.assertEquals(row.get(2, byte[].class), blob2);
                    break;
                case 4:
                    Assert.assertEquals(row.get(1, String.class), text3);
                    Assert.assertEquals(row.get(2, byte[].class), blob3);
                    break;
                default:
                    throw new RuntimeException("unknown row");

            }
            return row.asResultRow();
        };


        Mono.from(insertStmt.executeUpdate())
                .flatMapMany(s -> {
                    // LOG.info("affectedRows : {} , lastId : {}", s.affectedRows(), s.lastInsertedId());
                    final int rowCount = (int) s.affectedRows();
                    long lastId = s.lastInsertedId();
                    for (int i = 0; i < rowCount; i++) {
                        queryInsert.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    return Flux.from(queryInsert.executeQuery(function));
                })
                .blockLast();
    }


    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     *     bind {@link MySQLType#MEDIUMINT} and {@link MySQLType#MEDIUMINT_UNSIGNED}
     * </p>
     *
     * @see JdbdType#MEDIUMINT
     * @see JdbdType#MEDIUMINT_UNSIGNED
     * @see MySQLType#MEDIUMINT
     * @see MySQLType#MEDIUMINT_UNSIGNED
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/integer-types.html">Integer Types </a>
     */
    @Test(invocationCount = 3, dataProvider = "mediumIntStmtProvider")
    public void mediumInt(final BindSingleStatement insertStmt, final BindSingleStatement queryInsert) {
        insertStmt.bind(0, JdbdType.MEDIUMINT, null)
                .bind(1, JdbdType.MEDIUMINT_UNSIGNED, null)

                .bind(2, JdbdType.MEDIUMINT, 0)
                .bind(3, JdbdType.MEDIUMINT_UNSIGNED, 0)

                // min value
                .bind(4, JdbdType.MEDIUMINT, -0x80_00_00) // -8388608
                .bind(5, JdbdType.MEDIUMINT_UNSIGNED, 0)

                // max value
                .bind(6, JdbdType.MEDIUMINT, 0x7f_ff_ff)
                .bind(7, JdbdType.MEDIUMINT_UNSIGNED, 0xff_ff_ff)

                // other
                .bind(8, JdbdType.MEDIUMINT, 6666)
                .bind(9, JdbdType.MEDIUMINT_UNSIGNED, 8888);


        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            //LOG.info("row id : {}", row.get(0, Long.class));
            switch ((int) row.rowNumber()) {
                case 1: {
                    Assert.assertNull(row.get(1, Integer.class));
                    Assert.assertNull(row.get(2, Integer.class));
                }
                break;
                case 2: {
                    Assert.assertEquals(row.get(1, Integer.class), 0);
                    Assert.assertEquals(row.get(2, Integer.class), 0);
                }
                break;
                case 3: {
                    Assert.assertEquals(row.get(1, Integer.class), -0x80_00_00);
                    Assert.assertEquals(row.get(2, Integer.class), 0);
                }
                break;
                case 4: {
                    Assert.assertEquals(row.get(1, Integer.class), 0x7f_ff_ff);
                    Assert.assertEquals(row.get(2, Integer.class), 0xff_ff_ff);
                }
                break;
                case 5: {
                    Assert.assertEquals(row.get(1, Integer.class), 6666);
                    Assert.assertEquals(row.get(2, Integer.class), 8888);
                }
                break;
                default:
                    throw new RuntimeException("unknown row");

            }
            return row.asResultRow();
        };


        Mono.from(insertStmt.executeUpdate())
                .flatMapMany(s -> {
                    // LOG.info("affectedRows : {} , lastId : {}", s.affectedRows(), s.lastInsertedId());
                    final int rowCount = (int) s.affectedRows();
                    long lastId = s.lastInsertedId();
                    for (int i = 0; i < rowCount; i++) {
                        queryInsert.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    return Flux.from(queryInsert.executeQuery(function));
                })
                .blockLast();


    }


    /**
     * @see JdbdType#MEDIUMINT
     * @see JdbdType#MEDIUMINT_UNSIGNED
     * @see MySQLType#MEDIUMINT
     * @see MySQLType#MEDIUMINT_UNSIGNED
     */
    @DataProvider(name = "mediumIntStmtProvider", parallel = true)
    public final Object[][] mediumIntStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_mediumint,my_mediumint_unsigned) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)";
        querySql = "SELECT t.id,t.my_mediumint,t.my_mediumint_unsigned FROM mysql_types AS t WHERE t.id IN (?,?,?,?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }


    /**
     * @see JdbdType#BOOLEAN
     * @see MySQLType#BOOLEAN
     */
    @DataProvider(name = "booleanStmtProvider", parallel = true)
    public final Object[][] booleanStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_boolean) VALUES (?),(?),(?)";
        querySql = "SELECT t.id,t.my_boolean FROM mysql_types AS t WHERE t.id IN (?,?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }

    /**
     * @see JdbdType#YEAR
     * @see MySQLType#YEAR
     */
    @DataProvider(name = "yearStmtProvider", parallel = true)
    public final Object[][] yearStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_year) VALUES (?),(?),(?)";
        querySql = "SELECT t.id,t.my_year FROM mysql_types AS t WHERE t.id IN (?,?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }

    /**
     * @see JdbdType#TIMESTAMP
     * @see JdbdType#TIMESTAMP_WITH_TIMEZONE
     * @see MySQLType#DATETIME
     */
    @DataProvider(name = "datetimeStmtProvider", parallel = true)
    public final Object[][] datetimeStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_datetime,my_datetime6) VALUES (?,?),(?,?),(?,?),(?,?)";
        querySql = "SELECT t.id,t.my_datetime,t.my_datetime6 FROM mysql_types AS t WHERE t.id IN (?,?,?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }

    @DataProvider(name = "textStmtProvider", parallel = true)
    public final Object[][] textStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_text,my_blob) VALUES (?,?),(?,?),(?,?),(?,?)";
        querySql = "SELECT t.id,t.my_text,t.my_blob FROM mysql_types AS t WHERE t.id IN (?,?,?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }


}
