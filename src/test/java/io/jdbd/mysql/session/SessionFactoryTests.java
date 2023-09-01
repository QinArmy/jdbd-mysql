package io.jdbd.mysql.session;

import io.jdbd.pool.PoolLocalDatabaseSession;
import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.LocalDatabaseSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import java.net.URLEncoder;

/**
 * <p>
 * This class is test class of {@link MySQLDatabaseSessionFactory}.
 * </p>
 */
@Test
public class SessionFactoryTests extends SessionTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(SessionFactoryTests.class);


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_socket">socket</a>
     */
    @Test(enabled = false)
    public void unixDomainSocket() throws Exception {
        // /tmp/mysql.sock
        // select @@Global.socket;
        // https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_socket
        LOG.info(URLEncoder.encode("/tmp/mysql.sock", "utf-8"));
    }


    @Test//(invocationCount = 20_000, threadPoolSize = 20)
    public void localSession() {

        Flux.from(sessionFactory.localSession())
                .doOnError(error -> LOG.error("", error))
                // .repeat(10)
                .doOnNext(session -> LOG.debug("{}", session))
                .flatMap(LocalDatabaseSession::startTransaction)
                .doOnNext(session -> LOG.debug("{}", session))
                .flatMap(LocalDatabaseSession::commit)
                .map(PoolLocalDatabaseSession.class::cast)
                .flatMap(PoolLocalDatabaseSession::reset)
                .flatMap(session -> session.ping(200))
                .flatMap(DatabaseSession::close)
                .then()
                .block();

    }

    @Test
    public void rmSession() {

        Flux.from(sessionFactory.rmSession())
                .doOnError(error -> LOG.error("", error))
                //.repeat(8)
                .doOnNext(session -> LOG.debug("{}", session))
                .map(PoolRmDatabaseSession.class::cast)
                .flatMap(PoolRmDatabaseSession::reset)
                .flatMap(session -> session.ping(200))
                .flatMap(DatabaseSession::close)
                .blockLast();

    }


}
