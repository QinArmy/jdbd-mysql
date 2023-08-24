package io.jdbd.mysql.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * <p>
 * This class is test class of {@link MySQLDatabaseSessionFactory}.
 * </p>
 */
@Deprecated
@Test(enabled = false)
public class SessionFactorySuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(SessionFactorySuiteTests.class);

    /**
     * @see MySQLDatabaseSessionFactory#localSession()
     */
    @Test
    public void getTxSession() {
//        final Map<String, Object> configMap = ClientTestUtils.loadConfigMap();
//        configMap.put(MySQLKey.SSL_MODE.name, "DISABLED");
//        final DatabaseSessionFactory factory;
//      //  factory = DriverManager.forPoolVendor((String) configMap.get("url"), configMap);
//        final LocalDatabaseSession session;
//        session = Mono.from(factory.localSession())
//                .block();
//        assertNotNull(session, "session");
//        final StaticStatement statement = session.statement();
//        Mono.from(statement.executeUpdate("SET @@SESSION.session_track_transaction_info='CHARACTERISTICS'"))
//                .then(Mono.from(statement.executeUpdate("SET autocommit = 0")))
////                .thenMany(statement.executeQuery("SELECT 1 AS result"))
////                .then(Mono.from(statement.executeUpdate("UPDATE mysql_types AS t SET t.my_time = '00:01:00' WHERE t.id = 3")))
////                .then(Mono.from((statement.executeUpdate("INSERT INTO mysql_types(name) VALUES('army')"))))
////                .then(Mono.from((statement.executeUpdate("INSERT INTO mysql_types(name) VALUES('army')"))))
//                .then(Mono.from((statement.executeUpdate("COMMIT"))))
//                .then()
//                .block();

    }


}
