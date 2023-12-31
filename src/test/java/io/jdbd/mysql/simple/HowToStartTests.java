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

package io.jdbd.mysql.simple;

import io.jdbd.Driver;
import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.Isolation;
import io.jdbd.session.Option;
import io.jdbd.session.TransactionOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public class HowToStartTests {

    private static final Logger LOG = LoggerFactory.getLogger(HowToStartTests.class);

    private static DatabaseSessionFactory sessionFactory;


    @BeforeClass
    public void createSessionFactory() {
        final String url;
        final Map<String, Object> map = new HashMap<>();

        url = "jdbd:mysql://localhost:3306/army_test?factoryWorkerCount=30";

        map.put(Driver.USER, "army_w");
        map.put(Driver.PASSWORD, "army123");

        final DatabaseSessionFactory factory;
        sessionFactory = factory = Driver.findDriver(url).forDeveloper(url, map);
        SessionTestSupport.createMysqlTypeTableIfNeed(factory);
    }


    @Test
    public void howToStart() {
        final String sql;
        sql = "INSERT INTO mysql_types(my_boolean,my_bigint,my_datetime,my_datetime6,my_var_char200) VALUES (?,?,?,?,?)";
        final ResultStates resultStates;

        resultStates = Mono.from(sessionFactory.localSession())
                .flatMap(session -> session.bindStatement(sql)
                        .bind(0, JdbdType.BOOLEAN, true)
                        .bind(1, JdbdType.BIGINT, null)
                        .bind(2, JdbdType.TIMESTAMP, LocalDateTime.now())
                        .bind(3, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                        .bind(4, JdbdType.VARCHAR, "中国 QinArmy's jdbd \n \\ \t \" \032 \b \r '''  \\' ")
                        .executeUpdate(Mono::from)
                )
                .block();

        Assert.assertNotNull(resultStates);
        Assert.assertEquals(resultStates.affectedRows(), 1);
        LOG.info("auto generate id : {}", resultStates.lastInsertedId());
    }

    @Test
    public void localTransaction() {
        final String sql;
        sql = "INSERT INTO mysql_types(my_boolean,my_bigint,my_datetime,my_datetime6,my_var_char200) VALUES (?,?,?,?,?)";
        Mono.from(sessionFactory.localSession())

                .flatMap(session -> Mono.from(session.startTransaction(TransactionOption.option(Isolation.REPEATABLE_READ, false)))      // start new transaction
                        .doOnSuccess(s -> {
                            Assert.assertTrue(s.inTransaction()); // session in  transaction block
                            Assert.assertTrue(session.inTransaction()); // session in  transaction block
                        })
                        .flatMap(t -> session.bindStatement(sql)
                                .bind(0, JdbdType.BOOLEAN, true)
                                .bind(1, JdbdType.BIGINT, null)
                                .bind(2, JdbdType.TIMESTAMP, LocalDateTime.now())
                                .bind(3, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                                .bind(4, JdbdType.VARCHAR, "中国 QinArmy's jdbd \n \\ \t \" \032 \b \r '''  \\' ")
                                .executeUpdate(Mono::from))
                        .then(Mono.defer(() -> Mono.from(session.commit())))    // commit transaction
                        .then(Mono.from(session.close())) // close session if no error, driver don't send message to database server before subscribing
                        .onErrorResume(error -> Mono.from(session.close())  // close session when occur error
                                .then(Mono.error(error))
                        )

                )
                .block();
    }

    @Test
    public void localTransactionAndCommitChain() {
        final String sql;
        sql = "INSERT INTO mysql_types(my_boolean,my_bigint,my_datetime,my_datetime6,my_var_char200) VALUES (?,?,?,?,?)";
        Mono.from(sessionFactory.localSession())

                .flatMap(session -> Mono.from(session.startTransaction(TransactionOption.option(Isolation.REPEATABLE_READ, false)))      // start new transaction
                        .doOnSuccess(s -> {
                            Assert.assertTrue(s.inTransaction()); // session in  transaction block
                            Assert.assertTrue(session.inTransaction()); // session in  transaction block
                        })
                        .flatMap(t -> session.bindStatement(sql)
                                .bind(0, JdbdType.BOOLEAN, true)
                                .bind(1, JdbdType.BIGINT, null)
                                .bind(2, JdbdType.TIMESTAMP, LocalDateTime.now())
                                .bind(3, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                                .bind(4, JdbdType.VARCHAR, "中国 QinArmy's jdbd \n \\ \t \" \032 \b \r '''  \\' ")
                                .executeUpdate(Mono::from)
                        )
                        .then(Mono.defer(() -> Mono.from(session.commit(Option.singleFunc(Option.CHAIN, Boolean.TRUE)))))    // commit chain transaction
                        .doOnSuccess(o -> {
                            Assert.assertTrue(o.isPresent());
                            Assert.assertTrue(o.get().inTransaction()); // session in new transaction block
                        }).then(Mono.defer(() -> Mono.from(session.commit())))
                        .then(Mono.from(session.close())) // close session if no error, driver don't send message to database server before subscribing
                        .onErrorResume(error -> Mono.from(session.close())  // close session when occur error
                                .then(Mono.error(error))
                        )
                )
                .block();
    }

    @Test
    public void multiRowInsert() {
        final String sql;
        sql = "INSERT INTO mysql_types(my_boolean,my_bigint) VALUES (?,?),(?,?)";

        final ResultStates resultStates;
        resultStates = Mono.from(sessionFactory.localSession())
                .flatMap(session -> session.bindStatement(sql)
                        // first row
                        .bind(0, JdbdType.BOOLEAN, true)
                        .bind(1, JdbdType.BIGINT, 8888)
                        // second row
                        .bind(2, JdbdType.BOOLEAN, true)
                        .bind(3, JdbdType.BIGINT, 6666)
                        .executeUpdate(Mono::from)
                )
                .block();

        Assert.assertNotNull(resultStates);
        final long affectedRows = resultStates.affectedRows();
        Assert.assertEquals(affectedRows, 2);

        // NOTE : lastInsertId is first row id ,not last row id in multi-row insert syntax
        long lastInsertId = resultStates.lastInsertedId();
        for (int i = 0; i < affectedRows; i++) {
            LOG.info("number {} row id: {}", i + 1, lastInsertId);
            lastInsertId++;
        }

    }


    @Test
    public void unixDomainSocket() throws Exception {
        //  select @@Global.socket;  // default is  /tmp/mysql.sock
        final String mysqlSocketPath = "/tmp/mysql.sock";
        final String hostAddress = URLEncoder.encode(mysqlSocketPath, "utf-8");
        // hostAddress result is  /%2Ftmp%2Fmysql.sock
        final String url = "jdbd:mysql://%2Ftmp%2Fmysql.sock/army_test";

        final Map<String, Object> map = new HashMap<>();
        map.put(Driver.USER, "army_w");
        map.put(Driver.PASSWORD, "army123");
        // properties will override the properties of url.
        final DatabaseSessionFactory domainSocketSessionFactory;
        domainSocketSessionFactory = Driver.findDriver(url).forDeveloper(url, map);

        final ResultRow row;
        row = Mono.from(domainSocketSessionFactory.localSession())
                .flatMapMany(session -> Flux.from(session.executeQuery("SELECT current_timestamp AS now")))
                .blockLast();

        Assert.assertNotNull(row);
        Assert.assertNotNull(row.get(0, LocalDateTime.class));

    }


}
