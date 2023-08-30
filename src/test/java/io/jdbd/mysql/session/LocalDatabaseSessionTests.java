package io.jdbd.mysql.session;


import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * <p>
 * This class is the test class of {@link MySQLLocalDatabaseSession}
 * </p>
 * <p>
 * All test method's session parameter is created by {@link #createLocalSession(ITestNGMethod, ITestContext)},
 * and is closed by {@link #closeSessionAfterTest(Method, ITestContext)}
 * </p>
 */
@Test(dataProvider = "localSessionProvider")
public class LocalDatabaseSessionTests extends SessionTestSupport {


}
