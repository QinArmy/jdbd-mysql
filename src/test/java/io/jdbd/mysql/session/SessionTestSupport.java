package io.jdbd.mysql.session;

import io.jdbd.Driver;
import io.jdbd.mysql.ClientTestUtils;
import io.jdbd.mysql.TestKey;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.result.DataRow;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.statement.BindSingleStatement;
import io.jdbd.statement.Statement;
import io.jdbd.vendor.env.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

public abstract class SessionTestSupport {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());


    protected static DatabaseSessionFactory sessionFactory;


    @BeforeSuite
    public final void beforeSuiteCreateSessionFactory() throws Exception {
        if (sessionFactory != null) {
            return;
        }
        sessionFactory = createSessionFactory();

        // create table
        final Path path;
        path = Paths.get(ClientTestUtils.getTestResourcesPath().toString(), "ddl/mysqlTypes.sql");
        final String sql;
        sql = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        Mono.from(sessionFactory.localSession())
                .flatMap(session -> Mono.from(session.executeUpdate(sql)))
                .block();
    }

    @AfterSuite
    public final void afterSuiteCloseSessionFactory() {
        final DatabaseSessionFactory sessionFactory = SessionTestSupport.sessionFactory;


        if (sessionFactory != null && !sessionFactory.isClosed()) {
            final String sql = "DROP TABLE IF EXISTS my_types";
            Mono.from(sessionFactory.localSession())
                    .flatMap(session -> Mono.from(session.executeUpdate(sql)))
                    .doOnSuccess(s -> LOG.info("{} ; complete", sql))
                    .then(Mono.defer(() -> Mono.from(sessionFactory.close())))
                    .block();
        }

    }

    @AfterMethod
    public final void closeSessionAfterTest(final Method method, final ITestContext context) {
        boolean match = false;
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (DatabaseSession.class.isAssignableFrom(parameterType)
                    || Statement.class.isAssignableFrom(parameterType)) {
                match = true;
                break;
            }
        }
        if (!match) {
            return;
        }

        final String key;
        key = method.getDeclaringClass().getName() + '.' + method.getName() + "#session";

        final Object value;
        value = context.getAttribute(key);
        if (value instanceof DatabaseSession) {
            context.removeAttribute(key);
            Mono.from(((DatabaseSession) value).close())
                    .block();
        } else if (value instanceof TestSessionHolder && ((TestSessionHolder) value).close) {
            Mono.from(((TestSessionHolder) value).session.close())
                    .block();
        }
    }

    @DataProvider(name = "localSessionProvider", parallel = true)
    public final Object[][] createLocalSession(final ITestNGMethod targetMethod, final ITestContext context) {
        return createDatabaseSession(true, targetMethod, context);
    }

    @DataProvider(name = "rmSessionProvider", parallel = true)
    public final Object[][] createRmSession(final ITestNGMethod targetMethod, final ITestContext context) {
        return createDatabaseSession(false, targetMethod, context);
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
        final boolean closeSession;
        closeSession = currentInvocationCount == targetMethod.getInvocationCount();

        context.setAttribute(keyOfSession, new TestSessionHolder(session, closeSession));
        return statement;
    }


    private Object[][] createDatabaseSession(final boolean local, final ITestNGMethod targetMethod,
                                             final ITestContext context) {

        final int currentInvocationCount = targetMethod.getCurrentInvocationCount() + 1;

        final String methodName, keyOfSession;
        methodName = targetMethod.getMethodName();
        keyOfSession = targetMethod.getRealClass().getName() + '.' + methodName + "#session";

        final DatabaseSession session;
        if (local) {
            session = Mono.from(sessionFactory.localSession())
                    .block();
        } else {
            session = Mono.from(sessionFactory.rmSession())
                    .block();
        }
        Assert.assertNotNull(session);

        context.setAttribute(keyOfSession, session);

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
        //LOG.debug("driver {} ", driver);
        return driver.forPoolVendor(url, testEnv.sourceMap());
    }


    /*-------------------below static class  -------------------*/

    /**
     * for {@link #closeSessionAfterTest(Method, ITestContext)}
     */
    protected static final class TestSessionHolder {

        public final DatabaseSession session;

        public final boolean close;

        public TestSessionHolder(DatabaseSession session, boolean close) {
            this.session = session;
            this.close = close;
        }

    }// TestSessionHolder


}
