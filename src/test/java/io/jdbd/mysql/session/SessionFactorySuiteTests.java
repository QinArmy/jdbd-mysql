package io.jdbd.mysql.session;

import io.jdbd.Driver;
import io.jdbd.mysql.TestKey;
import io.jdbd.mysql.protocol.client.ClientTestUtils;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.vendor.env.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

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
        LOG.debug("{}", doCreateSessionFactory());
    }

    @Test
    public void localSession() {
        final DatabaseSessionFactory sessionFactory;
        sessionFactory = doCreateSessionFactory();

        final LocalDatabaseSession session;
        session = Mono.from(sessionFactory.localSession())
                .block();
        // SET character_set_connection = 'utf8mb4';
        // SET character_set_client 'utf8mb4';
        // SET character_set_results = NULL;
        // SET @@SESSION.sql_mode = DEFAULT , @@SESSION.time_zone = DEFAULT , @@SESSION.transaction_isolation = DEFAULT , @@SESSION.transaction_read_only = DEFAULT , @@SESSION.autocommit = DEFAULT;
        // SELECT DATE_FORMAT(FROM_UNIXTIME(1692914281),'%Y-%m-%d %T') AS databaseNow , @@SESSION.sql_mode AS sqlMode , @@SESSION.local_infile localInfile
        LOG.debug("session : {}", session);
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
