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

package io.jdbd.mysql.statement;

import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.ClientTestUtils;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.mysql.type.City;
import io.jdbd.mysql.util.MySQLArrays;
import io.jdbd.mysql.util.MySQLSpatials;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.statement.BindSingleStatement;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.type.BlobPath;
import io.jdbd.type.Point;
import io.jdbd.type.TextPath;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.ZoneOffset;
import java.util.BitSet;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * <p>
 * This class is responsible for test data type bind and get.
 * <br/>
 * <p>
 * All test method's session parameter is closed by {@link #closeSessionAfterTest(Method, ITestContext)}
 * <br/>
 */
public class DataTypeTests extends SessionTestSupport {


    private static final Charset WKT_CHARSET = StandardCharsets.US_ASCII;


    protected static Path bigColumnWkbPath, bigColumnWktPath;


    @BeforeSuite
    public void createBigColumnTempFile() throws IOException {
        if (ClientTestUtils.isNotDriverDeveloperComputer()) {
            return;
        }

        final Path dir;
        dir = Paths.get(ClientTestUtils.getModulePath().toString(), "target/big_column");

        if (Files.notExists(dir)) {
            Files.createDirectories(dir);
        }

        final int wkbPointNumber = 64 * 1024;
        final Path wkbPath, textPath;
        wkbPath = Files.createTempFile(dir, "Geometry", ".wkb");
        textPath = Files.createTempFile(dir, "Geometry", ".wkt");
        bigColumnWkbPath = wkbPath;
        bigColumnWktPath = textPath;

        // writ LineString
        final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(4096 + 9);
        final Random random = ThreadLocalRandom.current();
        try (FileChannel channel = FileChannel.open(wkbPath, StandardOpenOption.APPEND)) {

            buffer.writeByte(1); // Little Endian;
            buffer.writeIntLE(2); // wkbType
            buffer.writeIntLE(wkbPointNumber);  // numPoints

            for (int i = 0; i < wkbPointNumber; ) {
                buffer.writeLongLE(random.nextLong());
                buffer.writeLongLE(random.nextLong());
                i++;

                if ((i & 255) == 0) {
                    buffer.readBytes(channel, channel.position(), buffer.readableBytes());
                    buffer.clear();
                }

            }
            buffer.clear();
            LOG.info("{} size {} mb", wkbPath, channel.size() >> 20);
        } catch (Throwable e) {
            buffer.release();
            throw e;
        }


        final int wktPointNumber = 64 * 1024;
        try (FileChannel channel = FileChannel.open(textPath, StandardOpenOption.APPEND)) {
            buffer.writeBytes("LINESTRING(".getBytes(WKT_CHARSET));
            double d;
            for (int i = 0; i < wktPointNumber; i++) {
                if (i > 0) {
                    buffer.writeByte(Constants.COMMA);
                }
                d = Double.longBitsToDouble(random.nextLong());
                if (Double.isNaN(d)) {
                    d = 0.0d;
                }
                buffer.writeBytes(Double.toString(d).getBytes(WKT_CHARSET));

                buffer.writeByte(Constants.SPACE);

                d = Double.longBitsToDouble(random.nextLong());
                if (Double.isNaN(d)) {
                    d = 0.0d;
                }
                buffer.writeBytes(Double.toString(d).getBytes(WKT_CHARSET));

                if ((i & 127) == 0) {
                    buffer.readBytes(channel, channel.position(), buffer.readableBytes());
                    buffer.clear();
                }

            }// for

            buffer.writeByte(')');
            buffer.readBytes(channel, channel.position(), buffer.readableBytes());

            LOG.info("Big LineString WKT temp file {} size {} mb", textPath, channel.size() >> 20);
        } finally {
            buffer.release();
        }


    }

    @AfterSuite
    public void deleteBigColumnFile() throws IOException {
        Path path;

        path = bigColumnWkbPath;
        if (path != null) {
            Files.deleteIfExists(path);
        }

        path = bigColumnWktPath;
        if (path != null) {
            Files.deleteIfExists(path);
        }
    }


    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     *     bind {@link MySQLType#BOOLEAN}
     * <br/>
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
     * <br/>
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
     * <br/>
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
     * <br/>
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

        text1 = "中国 QinArmy's jdbd \n \\ \t \" \032 \b \r '''  \\' ";
        text2 = "中国 QinArmy(秦军) is a open source organization that create better framework";
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
     *     bind {@link MySQLType#SET}
     * <br/>
     *
     * @see MySQLType#SET
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/string-type-syntax.html">TEXT</a>
     */
    @Test(invocationCount = 3, dataProvider = "setStmtProvider")
    public void setType(final BindSingleStatement insertStmt, final BindSingleStatement queryInsert) {
        final Set<City> citySet;
        citySet = MySQLArrays.asUnmodifiableSet(City.BEIJING, City.SHANGHAI);

        final Set<String> cityNameSet;
        cityNameSet = MySQLArrays.asUnmodifiableSet(City.TAIBEI.name(), City.XIANGGANG.name());

        final String cityNameSetString;
        cityNameSetString = City.AOMENG.name() + ',' + City.SHANGHAI.name();


        insertStmt.bind(0, MySQLType.SET, null)
                .bind(1, MySQLType.SET, citySet)

                .bind(2, MySQLType.SET, cityNameSet)
                .bind(3, MySQLType.SET, cityNameSetString);

        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            // LOG.info("row id : {}", row.get(0, Long.class));
            switch ((int) row.rowNumber()) {
                case 1:
                    Assert.assertNull(row.get(1, String.class));
                    Assert.assertEquals(row.getSet(1, City.class), Collections.emptySet());
                    break;
                case 2:
                    Assert.assertEquals(row.getSet(1, City.class), citySet);
                    break;
                case 3:
                    Assert.assertEquals(row.getSet(1, String.class), cityNameSet);
                    break;
                case 4:
                    Assert.assertEquals(row.getSet(1, City.class), MySQLArrays.asUnmodifiableSet(City.AOMENG, City.SHANGHAI));
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
     * <br/>
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
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     *     bind {@link MySQLType#BIT}
     * <br/>
     *
     * @see JdbdType#BIT
     * @see JdbdType#VARBIT
     * @see MySQLType#BIT
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/bit-type.html"> Bit-Value Type - BIT </a>
     */
    @Test(invocationCount = 3, dataProvider = "bitStmtProvider")
    public void bitType(final BindSingleStatement insertStmt, final BindSingleStatement queryStmt) {
        insertStmt.bind(0, JdbdType.BIT, null)
                .bind(1, JdbdType.BIT, null)

                .bind(2, JdbdType.BIT, (byte) 0)
                .bind(3, JdbdType.BIT, 0)

                .bind(4, JdbdType.BIT, (1 << 20) - 1)
                .bind(5, JdbdType.BIT, -1L)

                .bind(6, JdbdType.BIT, 0x7f_f)
                .bind(7, JdbdType.BIT, BitSet.valueOf(new long[]{0xff_ff_ff}));


        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            //LOG.info("row id : {}", row.get(0, Long.class));
            switch ((int) row.rowNumber()) {
                case 1: {
                    Assert.assertNull(row.get(1, Long.class));
                    Assert.assertNull(row.get(2, Long.class));
                }
                break;
                case 2: {
                    Assert.assertEquals(row.get(1, Long.class), 0L);
                    Assert.assertEquals(row.get(2, Long.class), 0L);
                }
                break;
                case 3: {
                    Assert.assertEquals(row.get(1, Long.class), (1L << 20) - 1L);
                    Assert.assertEquals(row.get(2, Long.class), -1L);

                    Assert.assertEquals(row.get(2, BitSet.class), BitSet.valueOf(new long[]{-1L}));
                }
                break;
                case 4: {
                    Assert.assertEquals(row.get(1, Long.class), 0x7f_f);
                    Assert.assertEquals(row.get(2, Long.class), 0xff_ff_ff);
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
                        queryStmt.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    return Flux.from(queryStmt.executeQuery(function));
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
     *     bind {@link MySQLType#GEOMETRY}
     * <br/>
     *
     * @see JdbdType#GEOMETRY
     * @see MySQLType#GEOMETRY
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/spatial-types.html"> Spatial Data Types </a>
     */
    @Test(invocationCount = 3, dataProvider = "pointSmtProvider")
    public void pointType(final BindSingleStatement insertStmt, final BindSingleStatement queryStmt) {
        insertStmt.bind(0, JdbdType.GEOMETRY, null)
                .bind(1, JdbdType.GEOMETRY, null)

                .bind(2, JdbdType.GEOMETRY, Point.from(0, 0))
                .bind(3, JdbdType.GEOMETRY, Point.from(Double.MIN_VALUE, Double.MIN_VALUE))

                .bind(4, JdbdType.GEOMETRY, Point.from(Double.MAX_VALUE, Double.MAX_VALUE))
                .bind(5, JdbdType.GEOMETRY, Point.from(Double.MIN_VALUE, Double.MAX_VALUE))

                .bind(6, JdbdType.GEOMETRY, MySQLSpatials.writePointToWkb(false, Point.from(0, 0)))
                .bind(7, JdbdType.GEOMETRY, MySQLSpatials.writePointToWkb(false, Point.from(Double.MIN_VALUE, Double.MAX_VALUE)))

                .bind(8, JdbdType.GEOMETRY, MySQLSpatials.writePointToWkt(Point.from(0, 0)))
                .bind(9, JdbdType.GEOMETRY, MySQLSpatials.writePointToWkt(Point.from(Double.MIN_VALUE, Double.MAX_VALUE)));


        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            //LOG.info("row id : {}", row.get(0, Long.class));
            switch ((int) row.rowNumber()) {
                case 1: {
                    Assert.assertNull(row.get(1, Point.class));
                    Assert.assertNull(row.get(2, Point.class));
                }
                break;
                case 2: {
                    Assert.assertEquals(row.get(1, Point.class), Point.from(0, 0));
                    Assert.assertEquals(row.get(2, Point.class), Point.from(Double.MIN_VALUE, Double.MIN_VALUE));
                }
                break;
                case 3: {
                    Assert.assertEquals(row.get(1, Point.class), Point.from(Double.MAX_VALUE, Double.MAX_VALUE));
                    Assert.assertEquals(row.get(2, Point.class), Point.from(Double.MIN_VALUE, Double.MAX_VALUE));
                }
                break;
                case 4: {
                    Assert.assertEquals(row.get(1, byte[].class), MySQLSpatials.writePointToWkb(false, Point.from(0, 0)));
                    Assert.assertEquals(row.get(2, byte[].class), MySQLSpatials.writePointToWkb(false, Point.from(Double.MIN_VALUE, Double.MAX_VALUE)));
                }
                break;
                case 5: {
                    Assert.assertEquals(row.get(1, Point.class), Point.from(0, 0));
                    Assert.assertEquals(row.get(2, Point.class), Point.from(Double.MIN_VALUE, Double.MAX_VALUE));
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
                        queryStmt.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    return Flux.from(queryStmt.executeQuery(function));
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
     *     bind {@link MySQLType#GEOMETRY}
     * <br/>
     *
     * @see JdbdType#GEOMETRY
     * @see MySQLType#GEOMETRY
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/spatial-types.html"> Spatial Data Types </a>
     */
    @Test(invocationCount = 3, dataProvider = "bigColumnStmtProvider")
    public void bigColumn(final BindSingleStatement insertStmt, final BindSingleStatement queryStmt) {

        if (ClientTestUtils.isNotDriverDeveloperComputer()) {
            if (insertStmt instanceof PreparedStatement) {
                ((PreparedStatement) insertStmt).abandonBind();
            }
            if (queryStmt instanceof PreparedStatement) {
                ((PreparedStatement) queryStmt).abandonBind();
            }
            return;
        }
        final Path wkbPath = bigColumnWkbPath, wktPath = bigColumnWktPath;

        insertStmt.bind(0, JdbdType.GEOMETRY, BlobPath.from(false, wkbPath))
                .bind(1, JdbdType.GEOMETRY, wrapToBlob(wkbPath))

                .bind(2, JdbdType.GEOMETRY, TextPath.from(false, WKT_CHARSET, wktPath))
                .bind(3, JdbdType.GEOMETRY, wrapToClob(wktPath, WKT_CHARSET));

        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            //LOG.info("row id : {}", row.get(0, Long.class));
            try {
                BlobPath path;
                if (row.isBigColumn(1)) {
                    path = row.getNonNull(1, BlobPath.class);

                    LOG.info("{} size {} mb", path, Files.size(path.value()));
                }
                if (row.isBigColumn(2)) {
                    path = row.getNonNull(2, BlobPath.class);
                    LOG.info("{} size {} mb", path, Files.size(path.value()));
                }

            } catch (Throwable e) {
                LOG.error("asResultRow function error.", e);
                throw new RuntimeException(e);
            }

            return row.asResultRow();
        };


        Mono.from(insertStmt.executeUpdate())
                .flatMapMany(s -> {
                    // LOG.info("affectedRows : {} , lastId : {}", s.affectedRows(), s.lastInsertedId());
                    final int rowCount = (int) s.affectedRows();
                    LOG.info("big column test affectedRows {}", rowCount);
                    assert rowCount == 2; // TODO report MySQL bug
                    long lastId = s.lastInsertedId();
                    for (int i = 0; i < rowCount; i++) {
                        queryStmt.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    return Flux.from(queryStmt.executeQuery(function));
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
     *     bind {@link MySQLType#GEOMETRY} wkb
     * <br/>
     *
     * @see JdbdType#GEOMETRY
     * @see MySQLType#GEOMETRY
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/spatial-types.html"> Spatial Data Types </a>
     */
    @Test(invocationCount = 3, dataProvider = "bigBlobColumnStmtProvider")
    public void bigBlobColumn(final BindSingleStatement insertStmt, final BindSingleStatement queryStmt) {

        if (ClientTestUtils.isNotDriverDeveloperComputer()) {
            if (insertStmt instanceof PreparedStatement) {
                ((PreparedStatement) insertStmt).abandonBind();
            }
            if (queryStmt instanceof PreparedStatement) {
                ((PreparedStatement) queryStmt).abandonBind();
            }
            return;
        }

        final Path wkbPath = bigColumnWkbPath;

        insertStmt.bind(0, JdbdType.GEOMETRY, BlobPath.from(false, wkbPath))
                .bind(1, JdbdType.GEOMETRY, wrapToBlob(wkbPath));

        // insertStmt.setTimeout(50);

        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            //LOG.info("row id : {}", row.get(0, Long.class));
            try {
                if (row.isBigColumn(1)) {
                    BlobPath blobPath;
                    blobPath = row.getNonNull(1, BlobPath.class);
                    LOG.debug("rowNumber : {}  {} and {} singleBigColumn size match : {}", row.rowNumber(),
                            blobPath.value(), wkbPath, Files.size(blobPath.value()) == Files.size(wkbPath));
                } else {
                    row.getNonNull(1, byte[].class);
                }

            } catch (Throwable e) {
                LOG.error("asResultRow function error.", e);
                throw new RuntimeException(e);
            }

            return row.asResultRow();
        };


        Mono.from(insertStmt.executeUpdate())
                .flatMapMany(s -> {
                    LOG.info("affectedRows : {} , lastId : {}", s.affectedRows(), s.lastInsertedId());
                    final int rowCount = (int) s.affectedRows();
                    long lastId = s.lastInsertedId();
                    for (int i = 0; i < rowCount; i++) {
                        queryStmt.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    // queryStmt.setTimeout(300);
                    return Flux.from(queryStmt.executeQuery(function));
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
     *     bind {@link MySQLType#GEOMETRY} wkb
     * <br/>
     *
     * @see JdbdType#GEOMETRY
     * @see MySQLType#GEOMETRY
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/spatial-types.html"> Spatial Data Types </a>
     */
    @Test(invocationCount = 3, dataProvider = "bigClobColumnStmtProvider")
    public void bigClobColumn(final BindSingleStatement insertStmt, final BindSingleStatement queryStmt) {
        if (ClientTestUtils.isNotDriverDeveloperComputer()) {
            if (insertStmt instanceof PreparedStatement) {
                ((PreparedStatement) insertStmt).abandonBind();
            }
            if (queryStmt instanceof PreparedStatement) {
                ((PreparedStatement) queryStmt).abandonBind();
            }
            return;
        }
        final Path wktPath = bigColumnWktPath;
        insertStmt.bind(0, JdbdType.GEOMETRY, TextPath.from(false, WKT_CHARSET, wktPath))
                .bind(1, JdbdType.GEOMETRY, wrapToClob(wktPath, WKT_CHARSET));
        // .bind(1, JdbdType.GEOMETRY, wrapToText(wktPath,StandardCharsets.UTF_8));

        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            //LOG.info("row id : {}", row.get(0, Long.class));
            try {
//                BlobPath blobPath;
//                blobPath = row.getNonNull(1, BlobPath.class);
//                LOG.debug("rowNumber : {} bigClobColumn size {}", row.rowNumber(),Files.size(blobPath.value()));
                assert row.get(1) != null;
            } catch (Throwable e) {
                LOG.error("asResultRow function error.", e);
                throw new RuntimeException(e);
            }

            return row.asResultRow();
        };


        Mono.from(insertStmt.executeUpdate())
                .flatMapMany(s -> {
                    // LOG.info("affectedRows : {} , lastId : {}", s.affectedRows(), s.lastInsertedId());
                    final int rowCount = (int) s.affectedRows();
                    long lastId = s.lastInsertedId();
                    for (int i = 0; i < rowCount; i++) {
                        queryStmt.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    return Flux.from(queryStmt.executeQuery(function));
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
        querySql = "SELECT t.id,t.my_boolean,TRUE AS myTrue,FALSE AS myFalse,1 AS myNumber FROM mysql_types AS t WHERE t.id IN (?,?,?) ORDER BY t.id";

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

    @DataProvider(name = "setStmtProvider", parallel = true)
    public final Object[][] setStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_set) VALUES (?),(?),(?),(?)";
        querySql = "SELECT t.id,t.my_set FROM mysql_types AS t WHERE t.id IN (?,?,?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }

    @DataProvider(name = "bitStmtProvider", parallel = true)
    public final Object[][] bitStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_bit20,my_bit64) VALUES (?,?),(?,?),(?,?),(?,?)";
        querySql = "SELECT t.id,t.my_bit20,t.my_bit64 FROM mysql_types AS t WHERE t.id IN (?,?,?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }

    /**
     * @see #pointType(BindSingleStatement, BindSingleStatement)
     */
    @DataProvider(name = "pointSmtProvider", parallel = true)
    public final Object[][] pointSmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_point,my_geometry) VALUES (?,?),(st_geometryfromwkb(?),st_geometryfromwkb(?)),(st_geometryfromwkb(?),st_geometryfromwkb(?)),(st_geometryfromwkb(?),st_geometryfromwkb(?)),(st_geometryfromtext(?),st_geometryfromtext(?))";
        querySql = "SELECT t.id,t.my_point,t.my_geometry FROM mysql_types AS t WHERE t.id IN (?,?,?,?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }

    /**
     * @see #bigColumn(BindSingleStatement, BindSingleStatement)
     */
    @DataProvider(name = "bigColumnStmtProvider", parallel = true)
    public final Object[] bigColumnStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_linestring,my_geometry) VALUES (st_geometryfromwkb(?),st_geometryfromwkb(?)),(st_geometryfromtext(?),st_geometryfromtext(?))";
        querySql = "SELECT t.id,t.my_linestring,t.my_geometry,t.my_datetime6 FROM mysql_types AS t WHERE t.id IN (?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }

    /**
     * @see #bigBlobColumn(BindSingleStatement, BindSingleStatement)
     */
    @DataProvider(name = "bigBlobColumnStmtProvider", parallel = true)
    public final Object[] bigBlobColumnStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_geometry) VALUES (st_geometryfromwkb(?)),(st_geometryfromwkb(?))";
        querySql = "SELECT t.id,t.my_geometry FROM mysql_types AS t WHERE t.id IN (?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }

    /**
     * @see #bigClobColumn(BindSingleStatement, BindSingleStatement)
     */
    @DataProvider(name = "bigClobColumnStmtProvider", parallel = true)
    public final Object[] bigClobColumnStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_geometry) VALUES (st_geometryfromtext(?)),(st_geometryfromtext(?))";
        querySql = "SELECT t.id,t.my_geometry,t.my_datetime6 FROM mysql_types AS t WHERE t.id IN (?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }

}
