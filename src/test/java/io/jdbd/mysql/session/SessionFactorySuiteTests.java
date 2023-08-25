package io.jdbd.mysql.session;

import io.jdbd.Driver;
import io.jdbd.mysql.ClientTestUtils;
import io.jdbd.mysql.TestKey;
import io.jdbd.pool.PoolLocalDatabaseSession;
import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.env.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

/**
 * <p>
 * This class is test class of {@link MySQLDatabaseSessionFactory}.
 * </p>
 */
@Test
public class SessionFactorySuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(SessionFactorySuiteTests.class);


    private DatabaseSessionFactory sessionFactory;

    @BeforeClass
    public void beforeClass() {
        this.sessionFactory = createSessionFactory();
    }


    @Test//(invocationCount = 2000,threadPoolSize = 10)
    public void localSession() {
        final DatabaseSessionFactory sessionFactory;
        sessionFactory = this.sessionFactory;

        Flux.from(sessionFactory.localSession())
                // .repeat(10)
                .doOnNext(session -> LOG.debug("{}", session))
                .map(PoolLocalDatabaseSession.class::cast)
                .flatMap(PoolLocalDatabaseSession::reset)
                .flatMap(DatabaseSession::close)
                .then()
                .block();

    }

    @Test
    public void rmSession() {
        final DatabaseSessionFactory sessionFactory;
        sessionFactory = this.sessionFactory;

        Flux.from(sessionFactory.rmSession())
                //.repeat(8)
                .doOnNext(session -> LOG.debug("{}", session))
                .map(PoolRmDatabaseSession.class::cast)
                .flatMap(PoolRmDatabaseSession::reset)
                .flatMap(DatabaseSession::close)
                .blockLast();

    }


    private DatabaseSessionFactory createSessionFactory() {
        final Environment testEnv;
        testEnv = ClientTestUtils.getTestConfig();
        final String url;
        url = testEnv.getRequired(TestKey.URL);

        final Driver driver;
        driver = Driver.findDriver(url);
        //LOG.debug("driver {} ", driver);
        return driver.forPoolVendor(url, testEnv.sourceMap());
    }


}
