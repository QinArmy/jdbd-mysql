package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.type.City;
import io.jdbd.mysql.type.TrueOrFalse;
import io.jdbd.mysql.util.*;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static org.testng.Assert.*;

/**
 * <p>
 * This class is base of below:
 *     <ul>
 *         <li>{@link ComQueryDataTypeSuiteTests}</li>
 *     </ul>
 * </p>
 */
abstract class AbstractDataTypeSuiteTests extends AbstractTaskSuiteTests {

    private static final Queue<MySQLProtocol> PROTOCOL_QUEUE = new LinkedBlockingQueue<>();

    final Logger LOG = LoggerFactory.getLogger(getClass());


    private final long startId;

    AbstractDataTypeSuiteTests(long startId) {
        this.startId = startId;
    }

    abstract Mono<ResultStates> executeUpdate(ParamStmt stmt, TaskAdjutant adjutant);

    abstract Flux<ResultRow> executeQuery(ParamStmt stmt, TaskAdjutant adjutant);


    /**
     * @see MySQLType#TINYINT
     * @see MySQLType#TINYINT_UNSIGNED
     */
    final void tinyInt() {
        final long id = startId + 1;
        String column = "my_tinyint";
        MySQLType type = MySQLType.TINYINT;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Byte.MIN_VALUE);
        testType(id, column, type, Byte.MAX_VALUE);

        testType(id, column, type, (short) Byte.MIN_VALUE);
        testType(id, column, type, (short) Byte.MAX_VALUE);
        testType(id, column, type, (int) Byte.MIN_VALUE);
        testType(id, column, type, (int) Byte.MAX_VALUE);

        testType(id, column, type, (long) Byte.MIN_VALUE);
        testType(id, column, type, (long) Byte.MAX_VALUE);
        testType(id, column, type, BigInteger.valueOf(Byte.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Byte.MAX_VALUE));

        testType(id, column, type, BigDecimal.valueOf(Byte.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Byte.MAX_VALUE));
        testType(id, column, type, Byte.toString(Byte.MIN_VALUE));
        testType(id, column, type, Byte.toString(Byte.MAX_VALUE));


        column = "my_tinyint_unsigned";
        type = MySQLType.TINYINT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, (short) 0xFF);
        testType(id, column, type, 0xFF);

        testType(id, column, type, (long) 0xFF);
        testType(id, column, type, BigInteger.valueOf(0xFF));
        testType(id, column, type, BigDecimal.valueOf(0xFF));
        testType(id, column, type, Integer.toString(0xFF));
    }

    /**
     * @see MySQLType#SMALLINT
     * @see MySQLType#SMALLINT_UNSIGNED
     */
    final void smallInt() {
        final long id = startId + 2;
        String column;
        MySQLType type;

        column = "my_smallint";
        type = MySQLType.SMALLINT;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MIN_VALUE);
        testType(id, column, type, Short.MAX_VALUE);

        testType(id, column, type, (int) Short.MIN_VALUE);
        testType(id, column, type, (int) Short.MAX_VALUE);
        testType(id, column, type, (long) Short.MIN_VALUE);
        testType(id, column, type, (long) Short.MAX_VALUE);

        testType(id, column, type, BigInteger.valueOf(Short.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Short.MAX_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Short.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Short.MAX_VALUE));

        testType(id, column, type, Short.toString(Short.MIN_VALUE));
        testType(id, column, type, Short.toString(Short.MAX_VALUE));

        column = "my_smallint_unsigned";
        type = MySQLType.SMALLINT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, 0xFFFF);
        testType(id, column, type, (long) 0xFFFF);

        testType(id, column, type, (long) 0xFFFF);
        testType(id, column, type, BigInteger.valueOf(0xFFFF));
        testType(id, column, type, BigInteger.valueOf(0xFFFF));
        testType(id, column, type, BigDecimal.valueOf(0xFFFF));

        testType(id, column, type, BigDecimal.valueOf(0xFFFF));
        testType(id, column, type, Integer.toString(0xFFFF));
    }

    /**
     * @see MySQLType#MEDIUMINT
     * @see MySQLType#MEDIUMINT_UNSIGNED
     */
    final void mediumInt() {
        final long id = startId + 3;

        String column;
        MySQLType type;

        final int max = 0x7FFF_FF, min = (-max) - 1;
        column = "my_mediumint";
        type = MySQLType.MEDIUMINT;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, -1);
        testType(id, column, type, Short.MAX_VALUE);

        testType(id, column, type, min);
        testType(id, column, type, max);
        testType(id, column, type, (long) min);
        testType(id, column, type, (long) max);

        testType(id, column, type, BigInteger.valueOf(min));
        testType(id, column, type, BigInteger.valueOf(max));
        testType(id, column, type, BigDecimal.valueOf(min));
        testType(id, column, type, BigDecimal.valueOf(max));

        testType(id, column, type, Integer.toString(min));
        testType(id, column, type, Integer.toString(max));

        column = "my_mediumint_unsigned";
        type = MySQLType.MEDIUMINT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MAX_VALUE);
        testType(id, column, type, 0xFFFF_FF);

        testType(id, column, type, (long) 0xFFFF_FF);
        testType(id, column, type, BigInteger.valueOf(0xFFFF_FF));
        testType(id, column, type, BigDecimal.valueOf(0xFFFF_FF));
        testType(id, column, type, Integer.toString(0xFFFF_FF));

    }

    /**
     * @see MySQLType#INT
     * @see MySQLType#INT_UNSIGNED
     */
    final void integer() {
        final long id = startId + 4;

        String column;
        MySQLType type;

        column = "my_int";
        type = MySQLType.INT;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MIN_VALUE);
        testType(id, column, type, Short.MAX_VALUE);

        testType(id, column, type, Integer.MIN_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);
        testType(id, column, type, (long) Integer.MIN_VALUE);
        testType(id, column, type, (long) Integer.MAX_VALUE);

        testType(id, column, type, BigInteger.valueOf(Integer.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Integer.MAX_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Integer.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Integer.MAX_VALUE));

        testType(id, column, type, Integer.toString(Integer.MIN_VALUE));
        testType(id, column, type, Integer.toString(Integer.MAX_VALUE));

        column = "my_int_unsigned";
        type = MySQLType.INT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MAX_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);

        testType(id, column, type, 0xFFFF_FFFFL);
        testType(id, column, type, BigInteger.valueOf(0xFFFF_FFFFL));
        testType(id, column, type, BigDecimal.valueOf(0xFFFF_FFFFL));
        testType(id, column, type, Long.toString(0xFFFF_FFFFL));

    }

    /**
     * @see MySQLType#BIGINT
     * @see MySQLType#BIGINT_UNSIGNED
     */
    final void bigInt() {
        final long id = startId + 5;
        String column;
        MySQLType type;

        column = "my_bigint";
        type = MySQLType.BIGINT;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MIN_VALUE);
        testType(id, column, type, Short.MAX_VALUE);

        testType(id, column, type, Integer.MIN_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);
        testType(id, column, type, Long.MIN_VALUE);
        testType(id, column, type, Long.MAX_VALUE);

        testType(id, column, type, BigInteger.valueOf(Long.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Long.MAX_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Long.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Long.MAX_VALUE));

        testType(id, column, type, Long.toString(Long.MIN_VALUE));
        testType(id, column, type, Long.toString(Long.MAX_VALUE));


        column = "my_bigint_unsigned";
        type = MySQLType.BIGINT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MAX_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);

        testType(id, column, type, Long.MAX_VALUE);
        testType(id, column, type, MySQLNumbers.MAX_UNSIGNED_LONG);

        testType(id, column, type, new BigDecimal(MySQLNumbers.MAX_UNSIGNED_LONG));
        testType(id, column, type, MySQLNumbers.MAX_UNSIGNED_LONG.toString());
    }

    /**
     * @see MySQLType#DECIMAL
     * @see MySQLType#DECIMAL_UNSIGNED
     */
    final void decimal() {
        final long id = startId + 6;
        String column;
        MySQLType type;

        column = "my_decimal";
        type = MySQLType.DECIMAL;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);

        testType(id, column, type, Short.MIN_VALUE);
        testType(id, column, type, Short.MAX_VALUE);
        testType(id, column, type, Integer.MIN_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);

        testType(id, column, type, (long) Integer.MIN_VALUE);
        testType(id, column, type, (long) Integer.MAX_VALUE);
        testType(id, column, type, BigInteger.valueOf(Integer.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Integer.MAX_VALUE));

        testType(id, column, type, BigDecimal.valueOf(Integer.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Integer.MAX_VALUE));
        testType(id, column, type, new BigDecimal("0.00"));
        testType(id, column, type, new BigDecimal("2343.89"));

        testType(id, column, type, new BigDecimal("-2343.89"));
        testType(id, column, type, "-3423432.33");

        column = "my_decimal_unsigned";
        type = MySQLType.DECIMAL_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MAX_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);

        testType(id, column, type, (long) Integer.MAX_VALUE);
        testType(id, column, type, BigInteger.valueOf(Integer.MAX_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Integer.MAX_VALUE));
        testType(id, column, type, new BigDecimal("0.00"));

        testType(id, column, type, "3423432.33");
    }

    /**
     * @see MySQLType#FLOAT
     * @see MySQLType#FLOAT_UNSIGNED
     */
    @SuppressWarnings("deprecation")
    final void floatType() {
        final long id = startId + 7;
        String column;
        MySQLType type;

        column = "my_float";
        type = MySQLType.FLOAT;

        testType(id, column, type, null);
        testType(id, column, type, 0.0F);
        testType(id, column, type, -1F);
        testType(id, column, type, 1F);

        testType(id, column, type, "1.0E+36");
        testType(id, column, type, "-1.0E+36");

        column = "my_float_unsigned";
        type = MySQLType.FLOAT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, 0.0F);
        testType(id, column, type, 1F);
        testType(id, column, type, "1.0E+36");

    }

    /**
     * @see MySQLType#DOUBLE
     * @see MySQLType#DOUBLE_UNSIGNED
     */
    @SuppressWarnings("deprecation")
    final void doubleType() {
        final long id = startId + 8;
        String column;
        MySQLType type;

        column = "my_double";
        type = MySQLType.DOUBLE;

        testType(id, column, type, null);
        testType(id, column, type, 0.0D);
        testType(id, column, type, -1D);
        testType(id, column, type, 1D);

        testType(id, column, type, "1.0E+100");
        testType(id, column, type, "-1.0E+100");

        column = "my_double_unsigned";
        type = MySQLType.DOUBLE_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, 0.0D);
        testType(id, column, type, 1D);
        testType(id, column, type, "1.0E+100");

    }

    /**
     * @see MySQLType#CHAR
     */
    final void charType() {
        final long id = startId + 9;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _";
        String column;
        MySQLType type;

        column = "my_char200";
        type = MySQLType.CHAR;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);

    }

    /**
     * @see MySQLType#VARCHAR
     */
    final void varChar() {
        final long id = startId + 10;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        String column;
        MySQLType type;

        column = "my_var_char200";
        type = MySQLType.VARCHAR;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);

    }

    /**
     * @see MySQLType#BINARY
     */
    final void binary() {
        final long id = startId + 11;
        final StringBuilder builder = new StringBuilder(60);
        builder.append("army's name,; \\  \" 'text' '\032' \0 % _");

        for (int i = 0, end = 60 - builder.length(); i < end; i++) {
            builder.append(' ');
        }
        final String text = builder.toString();
        String column;
        MySQLType type;

        column = "my_binary60";
        type = MySQLType.BINARY;

        testType(id, column, type, null);
        testType(id, column, type, text);
        testType(id, column, type, text.getBytes(StandardCharsets.UTF_8));

    }

    /**
     * @see MySQLType#VARBINARY
     */
    final void varBinary() {
        final long id = startId + 12;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        String column;
        MySQLType type;

        column = "my_var_binary60";
        type = MySQLType.VARBINARY;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, text.getBytes(StandardCharsets.UTF_8));

    }

    /**
     * @see MySQLType#ENUM
     */
    final void enumType() {
        final long id = startId + 13;
        final String column = "my_enum";
        MySQLType type;

        type = MySQLType.ENUM;

        testType(id, column, type, null);
        testType(id, column, type, TrueOrFalse.T);
        testType(id, column, type, TrueOrFalse.F);
        testType(id, column, type, "T");

        testType(id, column, type, "F");
    }

    /**
     * @see MySQLType#SET
     */
    final void setType() {
        final long id = startId + 14;
        final String column = "my_set";
        MySQLType type;

        type = MySQLType.SET;

        testType(id, column, type, null);
        testType(id, column, type, MySQLArrays.asUnmodifiableSet(City.BEIJING));
        testType(id, column, type, MySQLArrays.asUnmodifiableSet(City.BEIJING, City.AOMENG, City.SHANGHAI));
        testType(id, column, type, MySQLArrays.asUnmodifiableSet(City.BEIJING.name()));

        testType(id, column, type, MySQLArrays.asUnmodifiableSet(City.BEIJING.name(), City.AOMENG.name(), City.SHANGHAI.name()));
    }

    /**
     * @see MySQLType#TIME
     */
    final void time() {
        final long id = startId + 15;
        String column;
        MySQLType type;

        column = "my_time";
        type = MySQLType.TIME;

        testType(id, column, type, null);
        testType(id, column, type, LocalTime.MIDNIGHT);
        testType(id, column, type, LocalTime.NOON);
        testType(id, column, type, LocalTime.parse("23:59:59"));

        testType(id, column, type, "23:59:59");

    }

    /**
     * @see MySQLType#DATE
     */
    final void date() {
        final long id = startId + 16;
        String column;
        MySQLType type;

        column = "my_date";
        type = MySQLType.DATE;

        testType(id, column, type, null);
        testType(id, column, type, LocalDate.now());
        testType(id, column, type, LocalDate.parse("1000-01-01"));
        testType(id, column, type, LocalDate.parse("9999-12-31"));

        testType(id, column, type, "1000-01-01");
        testType(id, column, type, "9999-12-31");
    }

    /**
     * @see MySQLType#TIMESTAMP
     */
    final void timestamp() {
        final long id = startId + 17;
        String column;
        MySQLType type;

        column = "my_timestamp";
        type = MySQLType.TIMESTAMP;

        testType(id, column, type, null);
        testType(id, column, type, LocalDateTime.parse("1970-01-02 00:00:01", MySQLTimes.ISO_LOCAL_DATETIME_FORMATTER));
        testType(id, column, type, LocalDateTime.parse("2038-01-19 03:14:07", MySQLTimes.ISO_LOCAL_DATETIME_FORMATTER));

        testType(id, column, type, "1970-01-02 00:00:01");// relate time_zone,so can't use '1970-01-01 00:00:01'
        testType(id, column, type, "2038-01-19 03:14:07");
    }

    /**
     * @see MySQLType#DATETIME
     */
    final void dateTime() {
        final long id = startId + 18;
        String column;
        MySQLType type;

        column = "my_datetime";
        type = MySQLType.DATETIME;

        testType(id, column, type, null);
        testType(id, column, type, LocalDateTime.parse("1000-01-01 00:00:00", MySQLTimes.ISO_LOCAL_DATETIME_FORMATTER));
        testType(id, column, type, LocalDateTime.parse("9999-12-31 23:59:59", MySQLTimes.ISO_LOCAL_DATETIME_FORMATTER));

        testType(id, column, type, "1000-01-01 00:00:00");// relate time_zone,so can't use '1970-01-01 00:00:01'
        testType(id, column, type, "9999-12-31 23:59:59");
    }

    /**
     * @see MySQLType#TINYTEXT
     */
    final void tinyText() {
        final long id = startId + 19;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        final String column = "my_tiny_text";
        final MySQLType type = MySQLType.TINYTEXT;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @see MySQLType#TEXT
     */
    final void text() {
        final long id = startId + 20;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        final String column = "my_text";
        final MySQLType type = MySQLType.TEXT;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @see MySQLType#MEDIUMTEXT
     */
    final void mediumText() {
        final long id = startId + 21;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        final String column = "my_medium_text";
        final MySQLType type = MySQLType.MEDIUMTEXT;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @see MySQLType#LONGTEXT
     */
    final void longText() throws IOException {
        final long id = startId + 22;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        final byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        final String column = "my_long_text";
        final MySQLType type = MySQLType.LONGTEXT;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, bytes);

        final Path path = Files.createTempFile("longText", "txt");

        try {
            try (OutputStream out = Files.newOutputStream(path, StandardOpenOption.WRITE)) {
                for (int i = 0; i < 2000; i++) {
                    out.write(bytes);
                }
            }

            testType(id, column, type, path);
        } finally {
            Files.deleteIfExists(path);
        }

    }


    /**
     * @see MySQLType#TINYBLOB
     */
    final void tinyBlob() {
        final long id = startId + 23;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        final String column = "my_tiny_blob";
        final MySQLType type = MySQLType.TINYBLOB;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @see MySQLType#BLOB
     */
    final void blob() {
        final long id = startId + 24;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        final String column = "my_blob";
        final MySQLType type = MySQLType.BLOB;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @see MySQLType#MEDIUMBLOB
     */
    final void mediumBlob() {
        final long id = startId + 25;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        final String column = "my_medium_blob";
        final MySQLType type = MySQLType.MEDIUMBLOB;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @see MySQLType#LONGBLOB
     */
    final void longBlob() throws IOException {
        final long id = startId + 26;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        final byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        final String column = "my_long_blob";
        final MySQLType type = MySQLType.LONGBLOB;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, bytes);

        LOG.debug("longBlob id:{}", id);
        final Path path = Files.createTempFile("longBlob", "txt");
        try {
            writeBigFile(path, bytes);
            testType(id, column, type, path);
        } finally {
            Files.deleteIfExists(path);
        }

    }

    final void writeBigFile(final Path path, final byte[] unitData) throws IOException {
        LOG.debug("temp file path:{}", path);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            final ByteBuffer buffer;
            if (unitData.length < 0xFF_FF_FF) {
                final byte[] chunk = new byte[0xFF_FF_FF];
                for (int i = 0, j = 0; i < chunk.length; i++, j++) {
                    if (j >= unitData.length) {
                        j = 0;
                    }
                    chunk[i] = unitData[j];
                }
                buffer = ByteBuffer.wrap(chunk);
            } else {
                buffer = ByteBuffer.wrap(unitData);
            }
            final int maxLength = (1 << 29);
            buffer.flip();
            for (int wroteBytes = 0; wroteBytes < maxLength; ) {
                wroteBytes += buffer.remaining();
                channel.write(buffer);
                buffer.clear();
            }
            LOG.debug("temp file size:{}", channel.size());
        }


    }

    /**
     * @see MySQLType#BIT
     */
    final void bitType() {
        final long id = startId + 27;
        final String column = "my_bit64";
        final MySQLType type = MySQLType.BIT;

        testType(id, column, type, null);
        //testType(id, column, type, -1L);
        testType(id, column, type, 0L);
        testType(id, column, type, 0B0101L);

        testType(id, column, type, 0xFF);

    }

    /**
     * @see MySQLType#YEAR
     */
    final void year() {
        final long id = startId + 28;
        final String column = "my_year";
        final MySQLType type = MySQLType.YEAR;

        testType(id, column, type, null);
        testType(id, column, type, Year.now());
        testType(id, column, type, (short) 1901);
        testType(id, column, type, (short) 2155);

        testType(id, column, type, 1901);
        testType(id, column, type, 2155);

    }

    /**
     * @see MySQLType#GEOMETRY
     */
    final void point() {
        final long id = startId + 29;
        final String column = "my_point";
        final MySQLType type = MySQLType.GEOMETRY;
        // Geometries.point()
    }

    private ResultRow testType(final long id, final String column, final MySQLType type, @Nullable final Object value) {

        String sql;
        sql = String.format("UPDATE mysql_types AS t SET t.%s = ? WHERE t.id = ? ", column);
        final List<ParamValue> bindGroup = MySQLCollections.arrayList(2);
        bindGroup.add(JdbdValues.paramValue(0, type, value));
        bindGroup.add(JdbdValues.paramValue(1, MySQLType.BIGINT, id));

        final ParamStmt updateStmt, queryStmt;
        updateStmt = Stmts.paramStmt(sql, bindGroup);
        sql = String.format("SELECT t.id,t.%s FROM mysql_types AS t WHERE t.id = ?", column);
        queryStmt = Stmts.single(sql, MySQLType.BIGINT, id);

        final ResultRow row;
        row = getClientProtocol()
                .flatMap(protocol -> executeStmt(protocol, updateStmt, queryStmt))
                .block();
        assertNotNull(row, "row");
        if (value == null) {
            assertNull(row.get(column), column);
        } else {
            assertResult(column, type, row, value);
        }
        return row;
    }

    @SuppressWarnings("deprecation")
    private void assertResult(final String column, final MySQLType type, final ResultRow row, final Object nonNull) {
        boolean useDefault = false;
        switch ((MySQLType) row.getRowMeta().getDataType(column)) {
            case SET: {
                final Set<?> valueSet = (Set<?>) nonNull;
                boolean isEnum = false;
                for (Object o : valueSet) {
                    isEnum = o instanceof City;
                    break;
                }

                final Set<?> result;
                if (isEnum) {
                    result = row.getSet(column, City.class);
                } else {
                    result = row.getSet(column, String.class);
                }
                assertEquals(result.size(), valueSet.size());
                assertEquals(result, valueSet, column);
            }
            break;
            case DECIMAL:
            case DECIMAL_UNSIGNED: {
                if (nonNull instanceof BigDecimal) {
                    assertEquals(row.getNonNull(column, BigDecimal.class).compareTo((BigDecimal) nonNull), 0, column);
                } else if (nonNull instanceof String) {
                    final BigDecimal r = row.getNonNull(column, BigDecimal.class);
                    assertEquals(r, new BigDecimal((String) nonNull), column);
                } else {
                    useDefault = true;
                }
            }
            break;
            case FLOAT:
            case FLOAT_UNSIGNED: {
                if (nonNull instanceof String) {
                    assertEquals(row.get(column, Float.class), Float.valueOf(Float.parseFloat((String) nonNull)), column);
                } else {
                    useDefault = true;
                }
            }
            break;
            case DOUBLE:
            case DOUBLE_UNSIGNED: {
                if (nonNull instanceof String) {
                    assertEquals(row.get(column, Double.class), Double.valueOf(Double.parseDouble((String) nonNull)), column);
                } else {
                    useDefault = true;
                }
            }
            break;
            case LONGTEXT: {
                if (!(nonNull instanceof Path)) {
                    useDefault = true;
                    break;
                }
                try {
                    String string = MySQLStreams.readAsString((Path) nonNull, StandardCharsets.UTF_8);
                    assertEquals(row.get(column, String.class), string, column);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            break;
            case LONGBLOB: {
                if (!(nonNull instanceof Path)) {
                    useDefault = true;
                    break;
                }
                try (FileChannel channel = FileChannel.open((Path) nonNull, StandardOpenOption.READ)) {
                    final byte[] bufferArray = new byte[(int) channel.size()];
                    final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);
                    channel.read(buffer);
                    buffer.flip();
                    assertEquals(row.get(column, byte[].class), bufferArray, column);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            break;
            default: {
                useDefault = true;
            }
        }

        if (useDefault) {
            final ResultRowMeta rowMeta = row.getRowMeta();
            assertEquals(rowMeta.getDataType(column), type, column);
            assertTrue(type.firstJavaType().isInstance(row.getNonNull(column)), column);
            assertEquals(row.get(column, nonNull.getClass()), nonNull, column);
        }

    }


    private Mono<ResultRow> executeStmt(final MySQLProtocol protocol, final ParamStmt updateStmt,
                                        final ParamStmt queryStmt) {
        final TaskAdjutant adjutant = getTaskAdjutant(protocol);
        return executeUpdate(updateStmt, adjutant)
                .switchIfEmpty(Mono.defer(this::updateFailure))
                .thenMany(executeQuery(queryStmt, adjutant))

                .concatWith(closeProtocol(protocol))
                .onErrorResume(error -> closeProtocol(protocol).then(Mono.error(error)))

                .last();
    }


    private <T> Mono<T> closeProtocol(final MySQLProtocol protocol) {
        return protocol.reset()
                .doOnSuccess(v -> PROTOCOL_QUEUE.offer(protocol))
                .then(Mono.empty());
    }


    private <T> Mono<T> updateFailure() {
        return Mono.error(new RuntimeException("update failure"));
    }

    Mono<MySQLProtocol> getClientProtocol() {
        final Mono<MySQLProtocol> mono;
        MySQLProtocol protocol;
        protocol = PROTOCOL_QUEUE.poll();
        if (protocol == null) {
            mono = PROTOCOL_FACTORY.createProtocol();
        } else {
            mono = Mono.just(protocol);
        }
        return mono;
    }


}
