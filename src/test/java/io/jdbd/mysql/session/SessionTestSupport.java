package io.jdbd.mysql.session;

import io.jdbd.Driver;
import io.jdbd.mysql.ClientTestUtils;
import io.jdbd.mysql.TestKey;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.result.DataRow;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.env.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

public abstract class SessionTestSupport {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());


    protected DatabaseSessionFactory sessionFactory;


    @BeforeClass
    public final void beforeClass() {
        if (this.sessionFactory == null) {
            this.sessionFactory = createSessionFactory();
        }

    }

    @AfterClass
    public final void afterClass() {
        Mono.from(this.sessionFactory.close())
                .block();
    }

    @AfterMethod
    public final void closeSessionAfterTest(final Method method, final ITestContext context) {
        boolean match = false;
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (DatabaseSession.class.isAssignableFrom(parameterType)) {
                match = true;
                break;
            }
        }
        if (!match) {
            return;
        }

        final String key;
        key = method.getDeclaringClass().getName() + '.' + method.getName();

        final Object value;
        value = context.removeAttribute(key);
        if (value instanceof DatabaseSession) {
            Mono.from(((DatabaseSession) value).close())
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


    private Object[][] createDatabaseSession(final boolean local, final ITestNGMethod targetMethod,
                                             final ITestContext context) {
        final DatabaseSession session;
        if (local) {
            session = Mono.from(this.sessionFactory.localSession())
                    .block();
        } else {
            session = Mono.from(this.sessionFactory.rmSession())
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


}
