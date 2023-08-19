package io.jdbd.mysql.util;

import io.jdbd.mysql.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalTime;

import static org.testng.Assert.*;

@Test(groups = {Groups.UTILS})
public class MySQLTimesSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLTimesSuiteTests.class);

    @BeforeClass
    public static void beforeClass() {
        LOG.info("\n {} test start\n", Groups.UTILS);
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("\n {} test end\n", Groups.UTILS);
    }


    @Test
    public void parseTimeAsDuration() {
        LOG.info("parseTimeAsDuration test start");

        long totalSecond, totalNanos;

        String timeText = "838:59:59";
        totalSecond = 838L * 3600L + 59 * 60 + 59;
        totalNanos = 0L;
        Duration duration = MySQLTimes.parseTimeAsDuration(timeText);
        assertNotNull(duration, "duration");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");

        timeText = "838:59:59.000000";
        totalSecond = 838L * 3600L + 59 * 60 + 59;
        totalNanos = 0L;

        duration = MySQLTimes.parseTimeAsDuration(timeText);

        assertNotNull(duration, "duration");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");

        timeText = "-838:59:59";
        totalSecond = -838L * 3600L - 59 * 60 - 59;
        totalNanos = 0L;

        duration = MySQLTimes.parseTimeAsDuration(timeText);

        assertNotNull(duration, "duration");
        assertTrue(duration.isNegative(), "isNegative");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");

        timeText = "-838:59:59.000000";
        totalSecond = -838L * 3600L - 59 * 60 - 59;
        totalNanos = 0L;

        duration = MySQLTimes.parseTimeAsDuration(timeText);

        assertNotNull(duration, "duration");
        assertTrue(duration.isNegative(), "isNegative");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");


        timeText = "23:59:59.999999";
        totalSecond = 23L * 3600L + 59L * 60L + 59L;
        totalNanos = 999_999L * 1000L;

        duration = MySQLTimes.parseTimeAsDuration(timeText);

        assertNotNull(duration, "duration");
        assertFalse(duration.isNegative(), "isNegative");
        assertEquals(duration.getSeconds(), totalSecond, "seconds");
        assertEquals(duration.getNano(), totalNanos, "totalNanos");


        LOG.info("parseTimeAsDuration test end");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText1() {
        LOG.info("errorDurationText1 test start");

        MySQLTimes.parseTimeAsDuration("343:-34:34");

        fail("errorDurationText1 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText2() {
        LOG.info("errorDurationText2 test start");

        MySQLTimes.parseTimeAsDuration("839:34:34");

        fail("errorDurationText2 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText3() {
        LOG.info("errorDurationText3 test start");

        MySQLTimes.parseTimeAsDuration("33:60:34");

        fail("errorDurationText3 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText4() {
        LOG.info("errorDurationText4 test start");

        MySQLTimes.parseTimeAsDuration("33:59:60");

        fail("errorDurationText4 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText5() {
        LOG.info("errorDurationText4 test start");

        MySQLTimes.parseTimeAsDuration("33:59:59.9999999");

        fail("errorDurationText4 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText6() {
        LOG.info("errorDurationText6 test start");

        MySQLTimes.parseTimeAsDuration("33:59:-59.999999");

        fail("errorDurationText6 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText7() {
        LOG.info("errorDurationText7 test start");

        MySQLTimes.parseTimeAsDuration("33:59:59.-9999");

        fail("errorDurationText7 failure.");
    }

    @Test(expectedExceptions = DateTimeException.class)
    public void errorDurationText8() {
        LOG.info("errorDurationText8 test start");

        MySQLTimes.parseTimeAsDuration("33:59:59.9999999");

        fail("errorDurationText8 failure.");
    }

    /**
     * test {@link MySQLTimes#convertToDuration(LocalTime)}
     */
    @Test
    public void convertLocalTimeToDuration() {
        LOG.info("convertLocalTimeToDuration test start");

        String timeText = "23:59:59.999999";
        LocalTime time = LocalTime.parse(timeText, MySQLTimes.MYSQL_TIME_FORMATTER);

        Duration duration = MySQLTimes.convertToDuration(time);
        long totalSeconds, totalNano;
        totalSeconds = 23L * 3600L + 59L * 60L + 59L;
        totalNano = 999_999L * 1000L;
        assertNotNull(duration, "duration");
        assertEquals(duration.getSeconds(), totalSeconds, "totalSeconds");
        assertEquals(duration.getNano(), totalNano, "totalNano");

        timeText = "00:00:00.000000";
        time = LocalTime.parse(timeText, MySQLTimes.MYSQL_TIME_FORMATTER);
        duration = MySQLTimes.convertToDuration(time);

        assertNotNull(duration, "duration");
        assertEquals(duration.getSeconds(), 0L, "totalSeconds");
        assertEquals(duration.getNano(), 0L, "totalNano");

        LOG.info("convertLocalTimeToDuration test end");
    }


    /**
     * @see MySQLTimes#durationToTimeText(Duration)
     */
    @Test(dependsOnMethods = "parseTimeAsDuration")
    public void durationToTimeText() {
        Duration duration;
        String text, timeText;

        duration = Duration.ZERO;
        timeText = MySQLTimes.durationToTimeText(duration);
        assertEquals(timeText, "00:00:00", duration.toString());

        text = "23:59:59.000000";
        duration = MySQLTimes.parseTimeAsDuration(text);
        timeText = MySQLTimes.durationToTimeText(duration);
        assertEquals(timeText, "23:59:59", text);

        text = "23:59:59.000001";
        duration = MySQLTimes.parseTimeAsDuration(text);
        timeText = MySQLTimes.durationToTimeText(duration);
        assertEquals(timeText, text, text);

        text = "838:59:59.000000";
        duration = MySQLTimes.parseTimeAsDuration(text);
        timeText = MySQLTimes.durationToTimeText(duration);
        assertEquals(timeText, "838:59:59", text);

        text = "-838:59:59.000000";
        duration = MySQLTimes.parseTimeAsDuration(text);
        timeText = MySQLTimes.durationToTimeText(duration);
        assertEquals(timeText, "-838:59:59", text);

    }

    @Test(dependsOnMethods = "parseTimeAsDuration", expectedExceptions = IllegalArgumentException.class)
    public void errorDurationToTimeText1() {
        Duration duration;
        duration = Duration.ofSeconds(MySQLTimes.DURATION_MAX_SECOND, 1L);
        MySQLTimes.durationToTimeText(duration);
        fail("errorDurationToTimeText1 failure.");

    }

    @Test(dependsOnMethods = "parseTimeAsDuration", expectedExceptions = IllegalArgumentException.class)
    public void errorDurationToTimeText2() {
        Duration duration;
        duration = Duration.ofSeconds(-MySQLTimes.DURATION_MAX_SECOND, 1L);
        MySQLTimes.durationToTimeText(duration);
        fail("errorDurationToTimeText2 failure.");

    }


}
