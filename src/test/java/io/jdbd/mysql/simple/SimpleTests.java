package io.jdbd.mysql.simple;

import io.jdbd.mysql.util.MySQLTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.time.OffsetDateTime;

public class SimpleTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTests.class);

    @Test
    public void test() {
        System.out.println(OffsetDateTime.now().format(MySQLTimes.OFFSET_DATETIME_FORMATTER_6));
    }

    private int binary(final int v) {
        return 0x80_00_00 | ((-v ^ 0x7F_FF_FF) + 1);
    }


}
