/*
 * Copyright 2023-2043 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jdbd.mysql.session;

import io.jdbd.pool.PoolLocalDatabaseSession;
import io.jdbd.pool.PoolRmDatabaseSession;
import io.jdbd.session.DatabaseSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URLEncoder;

/**
 * <p>
 * This class is test class of {@link MySQLDatabaseSessionFactory}.
 * <br/>
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


    @Test//(invocationCount = 20_000, threadPoolSize = 99)
    public void localSession() {

        Flux.from(sessionFactory.localSession())
                .doOnError(error -> LOG.error("", error))
                // .repeat(10)
                .doOnNext(session -> LOG.debug("{}", session))
                .flatMap(session -> Mono.from(session.startTransaction())
                        .doOnNext(info -> LOG.info("session info {}", info))
                        .then(Mono.from(session.commit()))
                        .map(PoolLocalDatabaseSession.class::cast)
                        .flatMap(s -> Mono.from(s.reset()))
                        .flatMap(s -> Mono.from(s.ping()))
                        .flatMap(s -> Mono.from(s.close()))
                )
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
                .flatMap(PoolRmDatabaseSession::ping)
                .flatMap(DatabaseSession::close)
                .blockLast();

    }


}
