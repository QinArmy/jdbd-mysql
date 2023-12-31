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

package io.jdbd.mysql.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.*;
import java.time.Year;
import java.util.Properties;

public class JdbcUnitTests {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcUnitTests.class);

    private static final String URL = "jdbc:mysql://localhost:3306/army_test";

    @Test
    public void statement() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL, createProperties())) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet resultSet = metaData.getFunctions(null, null, "%")) {
                while (resultSet.next()) {
                    System.out.printf("%s : %s\n", resultSet.getString("FUNCTION_NAME"), resultSet.getString("FUNCTION_TYPE"));
                }
            }
        }

    }

    @Test
    public void DatabaseMetaData() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL, createProperties())) {
            DatabaseMetaData metaData = conn.getMetaData();
            System.out.println(metaData.getSQLKeywords());
            try (ResultSet resultSet = metaData.getTypeInfo()) {
                while (resultSet.next()) {
                    System.out.printf("%s : %s\n", resultSet.getString("TYPE_NAME"), resultSet.getString("NUM_PREC_RADIX"));
                }
            }
        }

    }

    @Test
    public void prepare() throws SQLException {
        Properties prop = new Properties(createProperties());
        prop.put("useServerPrepStmts", "true");

        try (Connection conn = DriverManager.getConnection(URL, prop)) {
            String sql = "INSERT INTO mysql_types(my_year) VALUES(?),(?)";
            try (PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                stmt.setInt(1, Year.now().getValue());
                stmt.setInt(2, Year.now().getValue());
//                stmt.registerOutParameter(1, MysqlType.INT);
                int rows;
                rows = stmt.executeUpdate();
                LOG.info("update rows:{}", rows);
                try (ResultSet resultSet = stmt.getGeneratedKeys()) {
                    int count = 0;
                    while (resultSet.next()) {
                        LOG.info("insert id:{}", resultSet.getLong(1));
                        count++;
                    }
                    LOG.info("id count:{}", count);
                }

            }
        }
    }

    private static Properties createProperties() {
        Properties properties = new Properties();
        properties.put("user", "army_w");
        properties.put("password", "army123");
        properties.put("sslMode", "DISABLED");
        properties.put("useServerPrepStmts", "true");
        return properties;
    }


}
