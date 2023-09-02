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

#### more jdbd-spi document see [jdbd](https://github.com/QinArmy/jdbd "more jdbd-spi document")

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

@see
io/jdbd/mysql/simple/HowToStartTests.java ,[java class url](https://github.com/QinArmy/jdbd-mysql/blob/master/src/test/java/io/jdbd/mysql/simple/HowToStartTests.java "How to start")

### How to use native transport ?

maven
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
```

### Properties config

@see
io.jdbd.mysql.env.MySQLKey [class](https://github.com/QinArmy/jdbd-mysql/blob/master/src/main/java/io/jdbd/mysql/env/MySQLKey.java "io.jdbd.mysql.env.MySQLKey")

Core properties

| name                 | default                           | description                                                                                                           |
|----------------------|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| factoryName          | unnamed                           | DatabaseSessionFactory name                                                                                           |
| factoryTaskQueueSize | 18                                | The task queue size of each session.                                                                                  |
| factoryWorkerCount   | 30                                | session factory netty worker count.                                                                                   |
| factorySelectCount   | same with factoryWorkerCount      | session factory netty select count if use java NIO,if native ignore.                                                  |
| connectionProvider   | ConnectionProvider::newConnection | reactor netty ConnectionProvider                                                                                      |
| shutdownQuietPeriod  | 2 * 1000 milliseconds             | DatabaseSessionFactory's EventLoopGroup after n milliseconds   shutdown, if you invoke DatabaseSessionFactory.close() |
| shutdownTimeout      | 15 * 1000 milliseconds            | DatabaseSessionFactory's EventLoopGroup shutdown timeout milliseconds,if you invoke DatabaseSessionFactory.close()    |
| tcpKeepAlive         | false                             |                                                                                                                       |
| tcpNoDelay           | true                              |                                                                                                                       |
| connectTimeout       | 0                                 | session connect timeout                                                                                               |
| sslMode              | PREFERRED                         | ssl mode                                                                                                              |

#### more jdbd-spi document see [jdbd](https://github.com/QinArmy/jdbd "more jdbd-spi document")




