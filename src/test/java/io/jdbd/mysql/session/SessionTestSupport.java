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

import io.jdbd.Driver;
import io.jdbd.mysql.ClientTestUtils;
import io.jdbd.mysql.TestKey;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.result.DataRow;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.SessionHolderSpec;
import io.jdbd.statement.BindSingleStatement;
import io.jdbd.type.Blob;
import io.jdbd.type.Clob;
import io.jdbd.type.TextPath;
import io.jdbd.util.JdbdUtils;
import io.jdbd.vendor.env.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public abstract class SessionTestSupport {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected static final Executor EXECUTOR = Executors.newFixedThreadPool(3);
    protected static DatabaseSessionFactory sessionFactory;


    @BeforeSuite
    public final void beforeSuiteCreateSessionFactory() {
        if (sessionFactory != null) {
            return;
        }
        final DatabaseSessionFactory factory;
        sessionFactory = factory = createSessionFactory();
        createMysqlTypeTableIfNeed(factory);
    }

    @AfterSuite
    public final void afterSuiteCloseSessionFactory() {
        final DatabaseSessionFactory sessionFactory = SessionTestSupport.sessionFactory;


        if (sessionFactory != null && !sessionFactory.isClosed()) {

            final String sql = "TRUNCATE TABLE mysql_types";
            Mono.from(sessionFactory.localSession())
                    .flatMap(session -> Mono.from(session.executeUpdate(sql))
                            .then(Mono.from(session.close()))
                    )
                    .doOnSuccess(v -> LOG.info("{} ; complete", sql))
                    .then(Mono.defer(() -> Mono.from(sessionFactory.close())))
                    .block();
        }

    }


    @AfterMethod
    public final void closeSessionAfterTest(final ITestResult testResult) {
        final ITestNGMethod targetMethod = testResult.getMethod();

        final int invocationCount, currentInvocationCount;
        invocationCount = targetMethod.getInvocationCount();
        currentInvocationCount = targetMethod.getCurrentInvocationCount();

        for (Object parameter : testResult.getParameters()) {
            if (parameter instanceof DatabaseSession) {
                Mono.from(((DatabaseSession) parameter).close())
                        .block();
            } else if (parameter instanceof SessionHolderSpec && currentInvocationCount == invocationCount) {
                Mono.from(((SessionHolderSpec) parameter).getSession().close())
                        .block();
            }

        }
    }

    @DataProvider(name = "localSessionProvider", parallel = true)
    public final Object[][] createLocalSession(final ITestNGMethod targetMethod) {
        return createDatabaseSession(true, targetMethod);
    }

    @DataProvider(name = "rmSessionProvider", parallel = true)
    public final Object[][] createRmSession(final ITestNGMethod targetMethod) {
        return createDatabaseSession(false, targetMethod);
    }

    @DataProvider(name = "databaseMetadataProvider", parallel = true)
    public final Object[][] databaseMetadataProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String key;
        key = keyNameOfSession(targetMethod);

        final Object sessionValue;
        sessionValue = context.getAttribute(key);
        final DatabaseSession session;
        if (sessionValue instanceof DatabaseSession) {
            session = (DatabaseSession) sessionValue;
        } else {
            session = Mono.from(sessionFactory.localSession())
                    .block();
            assert session != null;
            context.setAttribute(key, session);
        }
        return new Object[][]{{session.databaseMetaData()}};
    }

    /**
     * @return a unmodified map
     */
    protected final Map<String, ?> mapCurrentRowToMap(final DataRow row) {
        final int columnCount = row.getColumnCount();

        final Map<String, Object> map = MySQLCollections.hashMap((int) (columnCount / 0.75f));
        for (int i = 0; i < columnCount; i++) {
            if (row.isNull(i)) {
                continue;
            }
            map.put(row.getColumnLabel(i), row.get(i));
        }
        return Collections.unmodifiableMap(map);
    }


    protected final BindSingleStatement createSingleStatement(final ITestNGMethod targetMethod, final ITestContext context,
                                                              final String sql) {
        Object temp;
        final String keyOfSession;
        keyOfSession = keyNameOfSession(targetMethod);

        temp = context.getAttribute(keyOfSession);

        final DatabaseSession session;
        if (temp instanceof TestSessionHolder) {
            session = ((TestSessionHolder) temp).session;
        } else {
            session = Mono.from(sessionFactory.localSession())
                    .block();
            Assert.assertNotNull(session);
        }

        final int currentInvocationCount = targetMethod.getCurrentInvocationCount() + 1;

        final BindSingleStatement statement;
        switch ((currentInvocationCount % 3)) {
            case 1:
                statement = session.bindStatement(sql);
                break;
            case 2:
                statement = session.bindStatement(sql, true);
                break;
            default:
                statement = Mono.from(session.prepareStatement(sql))
                        .block();
                Assert.assertNotNull(statement);

        }

        return statement;
    }


    private Object[][] createDatabaseSession(final boolean local, final ITestNGMethod targetMethod) {

        final int currentInvocationCount = targetMethod.getCurrentInvocationCount() + 1;

        final String methodName;
        methodName = targetMethod.getMethodName();

        final DatabaseSession session;
        if (local) {
            session = Mono.from(sessionFactory.localSession())
                    .block();
        } else {
            session = Mono.from(sessionFactory.rmSession())
                    .block();
        }
        Assert.assertNotNull(session);

        final Class<?>[] parameterTypeArray;
        parameterTypeArray = targetMethod.getParameterTypes();


        int sessionIndex = -1, methodIndex = -1, readOnlyIndex = -1;


        Class<?> parameterType;
        for (int i = 0; i < parameterTypeArray.length; i++) {
            parameterType = parameterTypeArray[i];
            if (DatabaseSession.class.isAssignableFrom(parameterType)) {
                sessionIndex = i;
            } else if (parameterType == boolean.class) {
                readOnlyIndex = i;
            } else if (parameterType == String.class) {
                methodIndex = i;
            }
        }

        final boolean readOnly = (currentInvocationCount & 1) == 0;

        final Object[][] result;
        if (sessionIndex > -1 && methodIndex > -1 && readOnlyIndex > -1) {
            result = new Object[1][3];
            result[0][sessionIndex] = session;
            result[0][methodIndex] = methodName;
            result[0][readOnlyIndex] = readOnly;
        } else if (sessionIndex > -1 && readOnlyIndex > -1) {
            result = new Object[1][2];
            result[0][sessionIndex] = session;
            result[0][readOnlyIndex] = readOnly;
        } else if (sessionIndex > -1 && methodIndex > -1) {
            result = new Object[1][2];
            result[0][sessionIndex] = session;
            result[0][methodIndex] = methodName;
        } else {
            result = new Object[][]{{session}};
        }
        return result;
    }

    /*-------------------below static method -------------------*/

    public static void createMysqlTypeTableIfNeed(final DatabaseSessionFactory sessionFactory) {
        try {
            // create table
            final Path path;
            path = Paths.get(ClientTestUtils.getTestResourcesPath().toString(), "ddl/mysqlTypes.sql");
            final String sql;
            sql = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);

            Mono.from(sessionFactory.localSession())
                    .flatMap(session -> Mono.from(session.executeUpdate(sql)))
                    .block();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static Blob wrapToBlob(final Path path) {
        final Flux<byte[]> flux;
        flux = Flux.create(sink -> EXECUTOR.execute(() -> publishBinaryFile(sink, path)));
        return Blob.from(flux);
    }


    public static Clob wrapToClob(final Path path, final Charset charset) {
        final Flux<String> flux;
        flux = Flux.create(sink -> EXECUTOR.execute(() -> publishTextFile(sink, path, charset)));
        return Clob.from(flux);
    }


    protected static String keyNameOfSession(final ITestNGMethod targetMethod) {
        return targetMethod.getRealClass().getName() + '.' + targetMethod.getMethodName() + "#session";
    }

    private static DatabaseSessionFactory createSessionFactory() {
        final Environment testEnv;
        testEnv = ClientTestUtils.getTestConfig();
        final String url;
        url = testEnv.getRequired(TestKey.URL);

        final Driver driver;
        driver = Driver.findDriver(url);

        final Map<String, Object> map;
        map = new HashMap<>(testEnv.sourceMap());

        if (!ClientTestUtils.isNotDriverDeveloperComputer()) {
            map.put("sslMode", "DISABLED");
        }

        //LOG.debug("driver {} ", driver);
        return driver.forPoolVendor(url, map);
    }


    private static void publishBinaryFile(final FluxSink<byte[]> sink, final Path path) {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            final byte[] bufferBytes = new byte[4096];
            final ByteBuffer buffer = ByteBuffer.wrap(bufferBytes);
            byte[] outBytes;
            for (int length; (length = channel.read(buffer)) > 0; ) {
                buffer.flip();
                outBytes = new byte[length];
                System.arraycopy(bufferBytes, 0, outBytes, 0, length);
                sink.next(outBytes);
                buffer.clear();
            }// for
            sink.complete();
        } catch (Throwable e) {
            sink.error(e);
        }
    }

    private static void publishTextFile(final FluxSink<String> sink, final Path path, final Charset charset) {
        try (BufferedReader reader = JdbdUtils.newBufferedReader(TextPath.from(false, charset, path), 8192)) {
            final CharBuffer charBuffer = CharBuffer.allocate(4096);

            while (reader.read(charBuffer) > 0) {
                charBuffer.flip();
                sink.next(charBuffer.toString());
                charBuffer.clear();
            }
            sink.complete();
        } catch (Throwable e) {
            sink.error(e);
        }
    }


    /*-------------------below static class  -------------------*/


    protected static final class TestSessionHolder {

        public final DatabaseSession session;

        public final boolean close;

        public TestSessionHolder(DatabaseSession session, boolean close) {
            this.session = session;
            this.close = close;
        }

    }// TestSessionHolder


}
