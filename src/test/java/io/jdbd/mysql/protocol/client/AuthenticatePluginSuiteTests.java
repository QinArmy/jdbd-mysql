package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.Groups;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.util.MySQLCollections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.*;
import reactor.netty.resources.LoopResources;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(groups = {Groups.AUTHENTICATE_PLUGIN}, dependsOnGroups = {Groups.MYSQL_URL, Groups.SQL_PARSER, Groups.UTILS})
public class AuthenticatePluginSuiteTests extends AbstractTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticatePluginSuiteTests.class);


    @BeforeSuite
    public static void beforeSuite(ITestContext context) {
        LOG.info("LoopResources {}", LoopResources.hasNativeSupport());
    }

    @AfterSuite
    public static void afterSuite(ITestContext context) {

    }

    @BeforeClass
    public static void beforeClass() {
        LOG.info("\n{} group test start.\n", Groups.AUTHENTICATE_PLUGIN);
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("\n{} group test end.\n", Groups.AUTHENTICATE_PLUGIN);
    }

    @Test(timeOut = TIME_OUT)
    public void cachingSha2PasswordPublicKeyAuthenticate() {
        final long startTime = System.currentTimeMillis();
        //TODO zoro add unit test
        LOG.info("cachingSha2PasswordPublicKeyAuthenticate test start.");
        final Path serverRSAPublicKeyPath;
        serverRSAPublicKeyPath = Paths.get(ClientTestUtils.getTestResourcesPath().toString()
                , "my-local/mysql-server/public_key.pem");

        if (Files.notExists(serverRSAPublicKeyPath)) {
            LOG.warn("{} not exists,ignore cachingSha2PasswordPublicKeyAuthenticate.", serverRSAPublicKeyPath);
            return;
        }

        final Map<String, Object> propMap;
        propMap = MySQLCollections.hashMap();

        propMap.put(MySQLKey.SSL_MODE.name, Enums.SslMode.DISABLED);
        // here use (CachingSha2PasswordPlugin and sslMode = DISABLED)
        propMap.put(MySQLKey.DISABLED_AUTHENTICATION_PLUGINS.name, CachingSha2PasswordPlugin.PLUGIN_NAME);
        propMap.put(MySQLKey.AUTHENTICATION_PLUGINS.name, CachingSha2PasswordPlugin.class.getName());
        propMap.put(MySQLKey.SERVER_RSA_PUBLIC_KEY_FILE.name, serverRSAPublicKeyPath.toString());

        // propMap.put(PropertyKey.allowPublicKeyRetrieval.getKey(), serverRSAPublicKeyPath.toString());

        try {
            AuthenticateResult result = MySQLTaskExecutor.create(createProtocolFactory(propMap))
                    .flatMap(executor -> MySQLConnectionTask.authenticate(executor.taskAdjutant()))
                    .block();

            assertNotNull(result, "result");
        } catch (Throwable e) {
            LOG.error("cachingSha2PasswordPublicKeyAuthenticate cost {}ms", System.currentTimeMillis() - startTime);
            throw e;
        }

        LOG.info("cachingSha2PasswordPublicKeyAuthenticate test end,cost {} ms", System.currentTimeMillis() - startTime);
    }


    @Test(dependsOnMethods = "cachingSha2PasswordPublicKeyAuthenticate", timeOut = TIME_OUT)
    public void defaultPlugin() throws Exception {
        LOG.info("defaultPlugin test start.");
        final Map<String, Object> propMap = MySQLCollections.hashMap();


        AuthenticateResult result = MySQLTaskExecutor.create(createProtocolFactory(propMap))
                .flatMap(executor -> MySQLConnectionTask.authenticate(executor.taskAdjutant()))
                .block();

        assertNotNull(result, "result");

        Handshake10 packet = result.handshakeV10Packet();
        assertNotNull(packet, "HandshakeV10Packet");

        assertTrue(result.capability() != 0, "negotiatedCapability");

        LOG.info("defaultPlugin test success. {}", packet);
    }


    @Test(dependsOnMethods = "defaultPlugin", timeOut = TIME_OUT)
    public void defaultPluginWithSslDisabled() {
        LOG.info("defaultPluginWithSslDisabled test start.");
        final Map<String, Object> propMap;
        propMap = Collections.singletonMap(MySQLKey.SSL_MODE.name, Enums.SslMode.DISABLED);


        AuthenticateResult result = MySQLTaskExecutor.create(createProtocolFactory(propMap))
                .flatMap(executor -> MySQLConnectionTask.authenticate(executor.taskAdjutant()))
                .block();

        assertNotNull(result, "result");
        LOG.info("defaultPluginWithSslDisabled test success.handshakeV10Packet:\n {}", result.handshakeV10Packet());

    }

    @Test(dependsOnMethods = "defaultPlugin", expectedExceptions = JdbdException.class)
    public void cachingSha2PasswordPluginEmptyPassword() {
        LOG.info("cachingSha2PasswordPluginEmptyPassword test start.");

        final Map<String, Object> propMap;
        propMap = MySQLCollections.hashMap();

        propMap.put(MySQLKey.SSL_MODE.name, Enums.SslMode.DISABLED.name());
        propMap.put(MySQLKey.DEFAULT_AUTHENTICATION_PLUGIN.name, CachingSha2PasswordPlugin.PLUGIN_NAME);
        propMap.put(MySQLKey.AUTHENTICATION_PLUGINS.name, CachingSha2PasswordPlugin.PLUGIN_NAME);
        propMap.put(MySQLKey.PASSWORD.name, "");

        AuthenticateResult result = MySQLTaskExecutor.create(createProtocolFactory(propMap))
                .flatMap(executor -> MySQLConnectionTask.authenticate(executor.taskAdjutant()))
                .block();

        assertNotNull(result, "result");

        Assert.fail("cachingSha2PasswordPluginEmptyPassword test failure.");
    }

    /**
     * This test need to config mysql serer.
     */
    @Test(enabled = false, dependsOnMethods = "defaultPlugin")
    public void mySQLNativePasswordPlugin() {
        LOG.info("mySQLNativePasswordPlugin test start.");

        final Map<String, Object> propMap;
        propMap = MySQLCollections.hashMap();

        propMap.put(MySQLKey.SSL_MODE.name, Enums.SslMode.DISABLED.name());
        propMap.put(MySQLKey.DEFAULT_AUTHENTICATION_PLUGIN.name, MySQLNativePasswordPlugin.PLUGIN_NAME);
        propMap.put(MySQLKey.AUTHENTICATION_PLUGINS.name, MySQLNativePasswordPlugin.PLUGIN_NAME);

        AuthenticateResult result = MySQLTaskExecutor.create(createProtocolFactory(propMap))
                .flatMap(executor -> MySQLConnectionTask.authenticate(executor.taskAdjutant()))
                .block();

        assertNotNull(result, "result");

        LOG.info("mySQLNativePasswordPlugin test end.");
    }


}
