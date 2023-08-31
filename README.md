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
5. If as of 8.0.26 , INSTALL COMPONENT "file://component_query_attributes" ;
6. mvn test


