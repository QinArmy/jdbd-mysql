package io.jdbd.mysql.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class SimpleTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTests.class);

    @Test
    public void simple() {
        System.out.println(0xff_ff_ff - 1);
    }


}
