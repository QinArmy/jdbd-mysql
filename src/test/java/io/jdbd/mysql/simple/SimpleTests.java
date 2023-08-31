package io.jdbd.mysql.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class SimpleTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTests.class);

    @Test
    public void test() {
        int max, min;
        max = 0x7fff_ff;
        min = -0x8000_00;
        LOG.info(Integer.toBinaryString(min));
        LOG.info("{}", Integer.toBinaryString(min).length());
        LOG.info(Integer.toBinaryString(binary(min)));
        LOG.info("{}", Integer.toBinaryString(binary(min)).length());
    }

    private int binary(final int v) {
        return 0x80_00_00 | ((-v ^ 0x7F_FF_FF) + 1);
    }


}
