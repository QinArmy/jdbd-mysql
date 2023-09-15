package io.jdbd.mysql.simple;

import io.jdbd.mysql.util.MySQLTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.time.ZoneOffset;

public class SimpleTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTests.class);

    @Test
    public void simple() throws Exception {
        LOG.info("{}", MySQLTimes.ZONE_FORMATTER.format(ZoneOffset.UTC));
    }


}
