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

import java.net.URLEncoder;

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


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_socket">socket</a>
     */
    @Test//(enabled = false)
    public void unixDomainSocket() throws Exception {
        // /tmp/mysql.sock
        // select @@Global.socket;
        // https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_socket
        LOG.info(URLEncoder.encode("/tmp/mysql.sock", "utf-8"));
    }

    @Test//(invocationCount = 10000, threadPoolSize = 30)
    public void localSession() {

        Flux.from(this.sessionFactory.localSession())
                .doOnError(error -> LOG.error("", error))
                // .repeat(10)
                .doOnNext(session -> LOG.debug("{}", session))
                .map(PoolLocalDatabaseSession.class::cast)
                .flatMap(PoolLocalDatabaseSession::reset)
                .flatMap(session -> session.ping(200))
                .flatMap(DatabaseSession::close)
                .then()
                .block();

    }

    @Test
    public void rmSession() {

        Flux.from(this.sessionFactory.rmSession())
                .doOnError(error -> LOG.error("", error))
                //.repeat(8)
                .doOnNext(session -> LOG.debug("{}", session))
                .map(PoolRmDatabaseSession.class::cast)
                .flatMap(PoolRmDatabaseSession::reset)
                .flatMap(session -> session.ping(200))
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
