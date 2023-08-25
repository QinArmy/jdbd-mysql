package io.jdbd.mysql.session;

import io.jdbd.Driver;
import io.jdbd.mysql.ClientTestUtils;
import io.jdbd.mysql.TestKey;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.env.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    /**
     * @see MySQLDatabaseSessionFactory#localSession()
     */
    @Test
    public void createSessionFactory() {
        final DatabaseSessionFactory sessionFactory;
        sessionFactory = doCreateSessionFactory();
        LOG.debug("{}", sessionFactory);

    }

    @Test
    public void localSession() {
        final DatabaseSessionFactory sessionFactory;
        sessionFactory = doCreateSessionFactory();

        Flux.from(sessionFactory.localSession())
                //.repeat(8)
                .doOnNext(session -> LOG.debug("{}", session))
                .flatMap(DatabaseSession::close)
                .blockLast();

    }


    private DatabaseSessionFactory doCreateSessionFactory() {
        final Environment testEnv;
        testEnv = ClientTestUtils.getTestConfig();
        final String url;
        url = testEnv.getRequired(TestKey.URL);

        final Driver driver;
        driver = Driver.findDriver(url);
        //LOG.debug("driver {} ", driver);
        return driver.forDeveloper(url, testEnv.sourceMap());
    }


}
