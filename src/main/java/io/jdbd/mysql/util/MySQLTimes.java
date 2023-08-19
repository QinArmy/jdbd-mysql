package io.jdbd.mysql.util;

import io.jdbd.vendor.util.JdbdTimes;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

import static java.time.temporal.ChronoField.*;

public abstract class MySQLTimes extends JdbdTimes {

    protected MySQLTimes() {
        throw new UnsupportedOperationException();
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time.html">The TIME Type</a>
     */
    public static final int DURATION_MAX_SECOND = 838 * 3600 + 59 * 60 + 59;


    /*################################## blow time formatter ##################################*/

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_0 = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)

            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_1 = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 1, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_2 = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 2, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_3 = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 3, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_4 = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 4, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER_5 = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 5, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(MYSQL_TIME_FORMATTER_0)
            .optionalStart()
            .appendFraction(MICRO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .toFormatter(Locale.ENGLISH);

    /*################################## blow datetime formatter ##################################*/

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_0 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_0)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_1 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_1)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_2 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_2)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_3 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_3)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_4 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_4)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER_5 = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER_5)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER)
            .toFormatter(Locale.ENGLISH);

    public static final DateTimeFormatter MYSQL_DATETIME_OFFSET_FORMATTER = new DateTimeFormatterBuilder()
            .append(MYSQL_DATETIME_FORMATTER)
            .appendOffset("+HH:MM", "+00:00")
            .toFormatter(Locale.ENGLISH);


    /**
     * @return true: timeText representing {@link Duration}.
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time.html">The TIME Type</a>
     */
    public static boolean isDuration(final String timeText) {
        int index = timeText.indexOf(':');
        final boolean duration;
        if (index < 0) {
            duration = false;
        } else {
            final int hours;
            hours = Integer.parseInt(timeText.substring(0, index));
            duration = hours < 0 || hours > 23;
        }
        return duration;
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time.html">The TIME Type</a>
     * @see #convertToDuration(LocalTime)
     */
    public static Duration parseTimeAsDuration(final String timeText) throws DateTimeException {

        try {
            final String[] itemArray = timeText.trim().split(":");
            if (itemArray.length != 3) {
                throw new DateTimeException(createTimeFormatErrorMessage(timeText));
            }
            final int hours, minutes, seconds, micros;
            hours = Integer.parseInt(itemArray[0]);
            minutes = Integer.parseInt(itemArray[1]);

            if (itemArray[2].contains(".")) {
                String[] secondPartArray = itemArray[2].split("\\.");
                if (secondPartArray.length != 2) {
                    throw new DateTimeException(createTimeFormatErrorMessage(timeText));
                }
                seconds = Integer.parseInt(secondPartArray[0]);
                micros = Integer.parseInt(secondPartArray[1]);
            } else {
                seconds = Integer.parseInt(itemArray[2]);
                micros = 0;
            }
            if (hours < -838 || hours > 838
                    || minutes < 0 || minutes > 59
                    || seconds < 0 || seconds > 59
                    || micros < 0 || micros > 999_999) {
                throw new DateTimeException(createTimeFormatErrorMessage(timeText));
            } else if (Math.abs(hours) == 838 && minutes == 59 && seconds == 59 && micros != 0) {
                throw new DateTimeException(createTimeFormatErrorMessage(timeText));
            }
            final long totalSecond;
            if (hours < 0) {
                totalSecond = (hours * 3600L) - (minutes * 60L) - seconds;
            } else {
                totalSecond = (hours * 3600L) + (minutes * 60L) + seconds;
            }
            //nanoAdjustment must be positive ,java.time.Duration.ofSeconds(long, long) method invoke java.lang.Math.floorDiv(long, long) cause bug.
            return Duration.ofSeconds(totalSecond, micros * 1000L);
        } catch (Throwable e) {
            throw new DateTimeException(createTimeFormatErrorMessage(timeText), e);
        }


    }


    public static boolean isOverflowDuration(final Duration duration) {
        final long abs = Math.abs(duration.getSeconds());
        return (abs > DURATION_MAX_SECOND) || (abs == DURATION_MAX_SECOND && duration.getNano() > 0L);
    }

    public static String durationToTimeText(final Duration duration) {
        if (isOverflowDuration(duration)) {
            throw new DateTimeException("duration too big,can't convert to MySQL TIME type.");
        }
        int restSecond = (int) Math.abs(duration.getSeconds());
        final int hours, minutes, seconds;
        hours = restSecond / 3600;
        restSecond %= 3600;
        minutes = restSecond / 60;
        seconds = restSecond % 60;

        StringBuilder builder = new StringBuilder(17);
        if (duration.isNegative()) {
            builder.append("-");
        }
        if (hours < 10) {
            builder.append("0");
        }
        builder.append(hours)
                .append(":");
        if (minutes < 10) {
            builder.append("0");
        }
        builder.append(minutes)
                .append(":");
        if (seconds < 10) {
            builder.append("0");
        }
        builder.append(seconds);

        final long micro = duration.getNano() / 1000L;
        if (micro > 999_999L) {
            throw new IllegalArgumentException(String.format("duration nano[%s] too big", duration.getNano()));
        }
        if (micro > 0L) {
            builder.append(".");
            String microText = Long.toString(micro);
            for (int i = 0, count = 6 - microText.length(); i < count; i++) {
                builder.append('0');
            }
            builder.append(microText);
        }
        return builder.toString();
    }

    /**
     * @param time {@link LocalTime} that underlying {@link java.time.ZoneOffset} match with database.
     * @see #parseTimeAsDuration(String)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time.html">The TIME Type</a>
     */
    public static Duration convertToDuration(LocalTime time) throws IllegalArgumentException {
        final long totalSecond;
        totalSecond = time.getHour() * 3600L + time.getMinute() * 60L + time.getSecond();
        return Duration.ofSeconds(totalSecond, time.getNano());
    }

    /**
     * @param timeText MySQL time text
     */
    public static DateTimeFormatter obtainTimeFormatterByText(String timeText) {
        final int index = timeText.lastIndexOf('.');
        return MySQLTimes.getTimeFormatter(index < 0 ? 0 : (timeText.length() - index - 1));
    }

    /**
     * @param dateTimeText MySQL datetime text
     */
    public static DateTimeFormatter obtainDateTimeFormatterByText(String dateTimeText) {
        final int index = dateTimeText.lastIndexOf('.');
        return MySQLTimes.getDateTimeFormatter(index < 0 ? 0 : (dateTimeText.length() - index - 1));
    }

    @Deprecated
    public static DateTimeFormatter getTimeFormatter(final int microPrecision) {
        final DateTimeFormatter formatter;
        switch (microPrecision) {
            case 0:
                formatter = MYSQL_TIME_FORMATTER_0;
                break;
            case 6:
                formatter = MYSQL_TIME_FORMATTER;
                break;
            case 1:
                formatter = MYSQL_TIME_FORMATTER_1;
                break;
            case 2:
                formatter = MYSQL_TIME_FORMATTER_2;
                break;
            case 3:
                formatter = MYSQL_TIME_FORMATTER_3;
                break;
            case 4:
                formatter = MYSQL_TIME_FORMATTER_4;
                break;
            case 5:
                formatter = MYSQL_TIME_FORMATTER_5;
                break;
            default:
                throw new IllegalArgumentException(String.format("microPrecision[%s] error", microPrecision));

        }

        return formatter;
    }

    @Deprecated
    public static DateTimeFormatter getDateTimeFormatter(final int microPrecision) {
        final DateTimeFormatter formatter;

        switch (microPrecision) {
            case 0:
                formatter = MYSQL_DATETIME_FORMATTER_0;
                break;
            case 6:
                formatter = MYSQL_DATETIME_FORMATTER;
                break;
            case 1:
                formatter = MYSQL_DATETIME_FORMATTER_1;
                break;
            case 2:
                formatter = MYSQL_DATETIME_FORMATTER_2;
                break;
            case 3:
                formatter = MYSQL_DATETIME_FORMATTER_3;
                break;
            case 4:
                formatter = MYSQL_DATETIME_FORMATTER_4;
                break;
            case 5:
                formatter = MYSQL_DATETIME_FORMATTER_5;
                break;
            default:
                throw new IllegalArgumentException(String.format("microPrecision[%s] error.", microPrecision));

        }

        return formatter;

    }


    private static String createTimeFormatErrorMessage(final String timeText) {
        return String.format("MySQL TIME[%s] format error.", timeText);
    }


}
