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
import io.jdbd.Driver;
import io.jdbd.session.DatabaseSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;

import java.util.HashMap;
import java.util.Map;

public class SimpleTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTests.class);

    private static DatabaseSessionFactory sessionFactory;

    @BeforeClass
    public void createSessionFactory(){
        if(sessionFactory == null){
            return;
        }
        String url;
        final Map<String,Object> map = new HashMap<>();

        url = "jdbd:mysql://localhost:3306/army_test?factoryWorkerCount=30";

        map.put(Driver.USER,"army_w");
        map.put(Driver.PASSWORD,"army123");

        sessionFactory =  Driver.findDriver(url).forDeveloper(url,map);
    }


}


```
