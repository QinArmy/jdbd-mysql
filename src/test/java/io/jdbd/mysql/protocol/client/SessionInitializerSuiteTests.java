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

package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Groups;
import org.testng.annotations.Test;

@Deprecated
@Test(enabled = false, groups = {Groups.SESSION_INITIALIZER}, dependsOnGroups = {Groups.AUTHENTICATE_PLUGIN})
public class SessionInitializerSuiteTests extends AbstractTaskSuiteTests {
//
//    private static final Logger LOG = LoggerFactory.getLogger(SessionInitializerSuiteTests.class);
//
//    private static final ConcurrentMap<Long, ClientProtocol0> protocolMap = new ConcurrentHashMap<>();
//
//    @BeforeClass
//    public static void beforeClass() {
//        LOG.info("\n {} group test start.\n", Groups.SESSION_INITIALIZER);
//    }
//
//    @AfterClass
//    public static void afterClass() {
//        LOG.info("\n {} group test end.\n", Groups.SESSION_INITIALIZER);
//        LOG.info("close {} ,size:{}", ClientProtocol0.class.getName(), protocolMap.size());
//
//        Flux.fromIterable(protocolMap.values())
//                .flatMap(ClientProtocol0::close)
//                .then()
//                .block();
//
//        protocolMap.clear();
//    }
//
//
//    @Test(timeOut = TIME_OUT)
//    public void connectAndInitializing() {
//        LOG.info("connectAndInitializing test start.");
//        doConnectionTest(Collections.singletonMap(MySQLKey.SSL_MODE.name, "DISABLED"));
//
//        LOG.info("connectAndInitializing test success.");
//
//    }
//
//    @Test(dependsOnMethods = "connectAndInitializing", timeOut = TIME_OUT)
//    public void detectCustomCollation() {
//        LOG.info("detectCustomCollation test start.");
//        final Map<String, String> propMap;
//        propMap = Collections.singletonMap(MySQLKey.DETECT_CUSTOM_COLLATIONS.name, "true");
//        doConnectionTest(propMap);
//        LOG.info("detectCustomCollation test success.");
//    }
//
//    /**
//     * @see SessionResetter#reset()
//     */
//    @Test(dependsOnMethods = "connectAndInitializing", timeOut = TIME_OUT)
//    public void sessionResetter() {
//        LOG.info("sessionResetter test start.");
//        final Map<String, String> propMap;
//        propMap = new HashMap<>();
//
//        propMap.put(MySQLKey.SESSION_VARIABLES.name, "autocommit=1, transaction_isolation='REPEATABLE-READ'");
//
//        doConnectionTest(propMap);
//        LOG.info("sessionResetter test success.");
//    }
//
//
//    @Test
//    public void configConnectionZone() {
//        LOG.info("configConnectionZone test start.");
//        TaskAdjutant adjutant;
//
//        final Map<String, String> propMap;
//        propMap = new HashMap<>();
//
//        propMap.put(MySQLKey.CONNECTION_TIME_ZONE.name, "SERVER");
//        propMap.put(MySQLKey.SESSION_VARIABLES.name, "time_zone='+04:14'");
//        adjutant = doConnectionTest(propMap);
//
//        ZoneOffset zoneOffset = ZoneOffset.of("+04:14");
//        ZoneOffset zoneOffsetDatabase = adjutant.serverZone();
//        ZoneOffset zoneOffsetClient = adjutant.connZone();
//
//        assertEquals(zoneOffsetClient, zoneOffsetDatabase, "zoneOffsetClient");
//        assertEquals(zoneOffsetDatabase, zoneOffset, "zoneOffsetDatabase");
//
//
//        propMap.put(MySQLKey.CONNECTION_TIME_ZONE.name, "LOCAL");
//        adjutant = doConnectionTest(propMap);
//        zoneOffsetClient = adjutant.connZone();
//        assertEquals(zoneOffsetClient, MySQLTimes.systemZoneOffset(), "zoneOffsetClient");
//
//        propMap.put(MySQLKey.CONNECTION_TIME_ZONE.name, "+03:17");
//        adjutant = doConnectionTest(propMap);
//        zoneOffsetClient = adjutant.connZone();
//        assertEquals(zoneOffsetClient, ZoneOffset.of("+03:17"), "zoneOffsetClient");
//
//        propMap.put(MySQLKey.CONNECTION_TIME_ZONE.name, "Australia/Sydney");
//        adjutant = doConnectionTest(propMap);
//        zoneOffsetClient = adjutant.connZone();
//        assertEquals(zoneOffsetClient, ZoneOffset.of("Australia/Sydney", ZoneOffset.SHORT_IDS), "zoneOffsetClient");
//
//        LOG.info("configConnectionZone test success.");
//
//    }
//
//
//    @Test
//    public void configSessionCharsets() {
//        LOG.info("configSessionCharsets test start.");
//        TaskAdjutant adjutant;
//
//        final Map<String, String> propMap;
//        propMap = MySQLCollections.hashMap();
//
//        propMap.put(MySQLKey.CHARACTER_SET_RESULTS.name, "GBK");
//        adjutant = doConnectionTest(propMap);
//        assertEquals(adjutant.getCharsetResults(), Charset.forName("GBK"));
//
//        propMap.remove(MySQLKey.CHARACTER_SET_RESULTS.name);
//        adjutant = doConnectionTest(propMap);
//        assertNull(adjutant.getCharsetResults(), "charset results");
//
//        propMap.put(MySQLKey.CHARACTER_ENCODING.name, StandardCharsets.UTF_8.name());
//        adjutant = doConnectionTest(propMap);
//        assertNull(adjutant.getCharsetResults(), "charset results");
//        assertEquals(adjutant.charsetClient(), StandardCharsets.UTF_8, "charset client");
//
//
//        propMap.put(MySQLKey.CONNECTION_COLLATION.name, "utf8mb4");
//        propMap.remove(MySQLKey.CHARACTER_SET_RESULTS.name);
//        adjutant = doConnectionTest(propMap);
//        assertNull(adjutant.getCharsetResults(), "charset results");
//        assertEquals(adjutant.charsetClient(), StandardCharsets.UTF_8, "charset client");
//        String sql = "SELECT @@character_set_connection as  characterSetConnection" +
//                ", @@character_set_results as characterSetResults," +
//                "@@character_set_client as characterSetClient";
//        ResultRow resultRow = ComQueryTask.query(Stmts.stmt(sql), CurrentRow.AS_RESULT_ROW, adjutant)
//                .elementAt(0)
//                .block();
//        assertNotNull(resultRow);
//
//        assertEquals(resultRow.get("characterSetConnection", String.class), "utf8mb4", "characterSetConnection");
//        assertEquals(resultRow.get("characterSetClient", String.class), "utf8mb4", "characterSetClient");
//        assertNull(resultRow.get("characterSetResults", String.class), "characterSetResults");
//
//        LOG.info("configSessionCharsets test success.");
//    }
//
//
//    @Test
//    public void configSqlMode() {
//        LOG.info("configSqlMode test start.");
//        TaskAdjutant adjutant;
//
//        final Map<String, String> propMap;
//        propMap = MySQLCollections.hashMap();
//
//        propMap.put(MySQLKey.APPEND_SQL_MODE.name, "TIME_TRUNCATE_FRACTIONAL");
//        adjutant = doConnectionTest(propMap);
//        SessionEnv server = adjutant.sessionEnv();
//
//        assertNotNull(server, "server");
//        assertTrue(server.containSqlMode(SQLMode.TIME_TRUNCATE_FRACTIONAL), "TIME_TRUNCATE_FRACTIONAL");
//
//        LOG.info("configSqlMode test success.");
//
//    }
//
//
//
//    /*################################## blow private method ##################################*/
//
//    private TaskAdjutant doConnectionTest(Map<String, String> propMap) {
//        Map<String, Object> map;
//        map = MySQLCollections.hashMap(ClientTestUtils.loadConfigMap());
//        map.putAll(propMap);
//
//        final List<MySQLHost> hostList;
//        hostList = MySQLUrlParser.parse((String) map.get("url"), map);
//
//        final ClientProtocol protocol;
//        protocol = ClientProtocolFactory.from(hostList.get(0))
//                .createProtocol()
//                .map(ClientProtocol.class::cast)
//                .block();
//        Assert.assertNotNull(protocol);
//        return protocol.adjutant;
//    }


}
