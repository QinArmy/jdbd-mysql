package io.jdbd.mysql.session;

import io.jdbd.Driver;
import io.jdbd.mysql.ClientTestUtils;
import io.jdbd.mysql.TestKey;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.RmDatabaseSession;
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

public abstract class SessionTestSupport {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());


    protected DatabaseSessionFactory sessionFactory;


    @BeforeClass
    public final void beforeClass() {
        this.sessionFactory = createSessionFactory();
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
        final LocalDatabaseSession session;
        session = Mono.from(this.sessionFactory.localSession())
                .block();

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

    @DataProvider(name = "rmSessionProvider", parallel = true)
    public final Object[][] createRmSession() {
        final RmDatabaseSession session;
        session = Mono.from(this.sessionFactory.rmSession())
                .block();
        return new Object[][]{{session}};
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
