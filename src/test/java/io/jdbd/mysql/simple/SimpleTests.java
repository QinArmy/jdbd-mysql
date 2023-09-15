package io.jdbd.mysql.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class SimpleTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTests.class);

    @Test
    public void simple() throws Exception {
        Mono.just(1)
                .delayElement(Duration.ofSeconds(5))
                .doOnNext(s -> LOG.info("delay end"))
                .subscribe();

        Thread.currentThread().join(15 * 1000);
        LOG.info("simple end");

    }


}
