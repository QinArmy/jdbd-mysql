package io.jdbd.mysql.session;

import io.jdbd.Driver;
import io.jdbd.lang.Nullable;
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
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

public abstract class SessionTestSupport {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());


    protected static DatabaseSessionFactory sessionFactory;


    @BeforeSuite
    public final void beforeSuiteCreateSessionFactory() {
        if (sessionFactory == null) {
            sessionFactory = createSessionFactory();
        }

    }

    @AfterSuite
    public final void afterSuiteCloseSessionFactory() {
        DatabaseSessionFactory sessionFactory = SessionTestSupport.sessionFactory;

        if (sessionFactory != null) {
            Mono.from(sessionFactory.close())
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

    /**
     * @return get cache for data provider.
     */
    @Nullable
    protected final BindSingleStatement invokeDataProvider(final ITestNGMethod targetMethod, final ITestContext context,
                                                           final Function<DatabaseSession, BindSingleStatement> func) {
        Object temp;
        final String keyOfSession;
        keyOfSession = keyNameOfSession(targetMethod);

        temp = context.getAttribute(keyOfSession);

        final DatabaseSession cacheSession;
        if (temp instanceof TestSessionHolder) {
            cacheSession = ((TestSessionHolder) temp).session;
        } else {
            cacheSession = null;
        }

        final BindSingleStatement statement;
        statement = func.apply(cacheSession);

        final int currentInvocationCount = targetMethod.getCurrentInvocationCount();

        final boolean closeSession;
        closeSession = currentInvocationCount + 1 == targetMethod.getInvocationCount();

        final TestSessionHolder sessionHolder;
        if (cacheSession == null) {
            sessionHolder = new TestSessionHolder(statement.getSession(), closeSession);
        } else {
            sessionHolder = new TestSessionHolder(cacheSession, closeSession);
        }
        context.setAttribute(keyOfSession, sessionHolder);
        return statement;
    }


    private Object[][] createDatabaseSession(final boolean local, final ITestNGMethod targetMethod,
                                             final ITestContext context) {
        final DatabaseSession session;
        if (local) {
            session = Mono.from(sessionFactory.localSession())
                    .block();
        } else {
            session = Mono.from(sessionFactory.rmSession())
                    .block();
        }


        final String methodName, key;
        methodName = targetMethod.getMethodName();
        key = targetMethod.getRealClass().getName() + '.' + methodName;

        context.setAttribute(key, session);

        final Class<?>[] parameterTypeArray;
        parameterTypeArray = targetMethod.getParameterTypes();

        final boolean methodNameParameter = parameterTypeArray.length > 1 && parameterTypeArray[1] == String.class;

        final Object[][] result;
        if (methodNameParameter) {
            result = new Object[][]{{session, methodName}};
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
