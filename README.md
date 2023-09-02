# Reactive Relational Database Driver MySQL Implementation

This project contains the [MySQL client protocol][m] implementation of the [JDBD SPI][j].
This implementation is not intended to be used directly, but rather to be used as the backing implementation for
a humane client library to delegate to.

[m]: https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html

[j]: https://github.com/QinArmy/jdbd

### How to run jdbd-mysql test use cases ?

1. CREATE DATABASE army_test DEFAULT CHARACTER SET utf8mb4 ;
2. CREATE USER army_w@'localhost' IDENTIFIED BY 'army123' ;
3. GRANT ALL ON army_test.* TO army_w@'localhost' ;
4. GRANT XA_RECOVER_ADMIN ON &#42;.&#42; TO army_w@'localhost';
5. If meets MySQL 8.0.26 ,then INSTALL COMPONENT "file://component_query_attributes" ;
6. mvn test

### How to start ?

#### Maven

```xml

<dependency>
  <groupId>io.jdbd.mysql</groupId>
  <artifactId>jdbd-mysql</artifactId>
  <version>0.8.0</version>
</dependency>
```

#### DataBaseSessionFactory

```java
package io.jdbd.mysql.simple;

import io.jdbd.Driver;
import io.jdbd.meta.JdbdType;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public class SimpleTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTests.class);

    private static DatabaseSessionFactory sessionFactory;

    @BeforeClass
    public void createSessionFactory() {
        final String url;
        final Map<String, Object> map = new HashMap<>();

        url = "jdbd:mysql://localhost:3306/army_test?factoryWorkerCount=30";

        map.put(Driver.USER, "army_w");
        map.put(Driver.PASSWORD, "army123");

        sessionFactory = Driver.findDriver(url).forDeveloper(url, map);
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

        long lastInsertId = resultStates.lastInsertedId(); // NOTE : lastInsertId is first row id ,not last row id
        for (int i = 0; i < affectedRows; i++) {
            LOG.info("number {} row id: {}", i + 1, lastInsertId);
            lastInsertId++;
        }

    }


}

```
