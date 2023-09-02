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

#### Java code

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
        // NOTE : lastInsertId is first row id ,not last row id in multi-row insert syntax
        long lastInsertId = resultStates.lastInsertedId();
        for (int i = 0; i < affectedRows; i++) {
            LOG.info("number {} row id: {}", i + 1, lastInsertId);
            lastInsertId++;
        }

    }


}

```

### How to use native transport ?

```xml

<dependencies>
    <dependency>
        <groupId>io.netty</groupId>
        <artifactId>netty-transport-native-kqueue</artifactId>
        <version>4.1.96.Final</version>
        <classifier>osx-x86_64</classifier>  <!-- here,just for mac os  -->
    </dependency>
    <dependency>
        <groupId>io.netty</groupId>
        <artifactId>netty-transport-native-unix-common</artifactId>
        <version>4.1.96.Final</version>
        <classifier>osx-x86_64</classifier>   <!-- here,just for mac os  -->
    </dependency>
</dependencies>

```

### How to use unix domain socket ?

```java
public class UnixDomainSocketTests {

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

        DatabaseSessionFactory sessionFactory;
        sessionFactory = Driver.findDriver(url).forDeveloper(url, map);

    }
}
```

### Properties config

@see io.jdbd.mysql.env.MySQLKey

Core properties

| name                 | default                           | description                                                             |
|----------------------|-----------------------------------|-------------------------------------------------------------------------|
| factoryName          | unnamed                           | DatabaseSessionFactory name                                             |
| factoryTaskQueueSize | 18                                | The task queue size of each session.                                    |
| factoryWorkerCount   | 30                                | session factory netty worker count.                                     |
| factorySelectCount   | same with factoryWorkerCount      | session factory netty select count if use java NIO,if native ignore.    |
| connectionProvider   | ConnectionProvider::newConnection | reactor netty ConnectionProvider                                        |
| shutdownQuietPeriod  | 2 * 1000 milliseconds             | DatabaseSessionFactory's EventLoopGroup after n milliseconds   shutdown |
| shutdownTimeout      | 15 * 1000 milliseconds            | DatabaseSessionFactory's EventLoopGroup shutdown timeout milliseconds   |
| tcpKeepAlive         | false                             |                                                                         |
| tcpNoDelay           | true                              |                                                                         |
| connectTimeout       | 0                                 | session connect timeout                                                 |
| sslMode              | PREFERRED                         | ssl mode                                                                |

### more jdbd-spi document see [jdbd](https://github.com/QinArmy/jdbd "more jdbd-spi document")




