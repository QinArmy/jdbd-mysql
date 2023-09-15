package io.jdbd.mysql.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class ReportBugTests {

    private static final Logger LOG = LoggerFactory.getLogger(ReportBugTests.class);

    private static final Executor EXECUTOR = Executors.newFixedThreadPool(1);


    @Test
    public void reportBug() {
        final Consumer<FluxSink<Integer>> consumer = sink -> EXECUTOR.execute(() -> {
            
            for (int i = 255; i < 300; i++) {
                sink.next(i);
            }
            sink.complete();
        });
        Flux.create(consumer)
                .filter(num -> {
                    LOG.info("num : {}", num);
                    return num < 100;
                })
                .blockLast();
    }

}
