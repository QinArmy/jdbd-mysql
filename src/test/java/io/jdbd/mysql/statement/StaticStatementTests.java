package io.jdbd.mysql.statement;

import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.statement.StaticStatement;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;


/**
 * <p>
 * This class is the test class of {@link StaticStatement}
 * </p>
 * <p>
 * All test method's session parameter is created by {@link #createLocalSession(ITestNGMethod, ITestContext)},
 * and is closed by {@link #closeSessionAfterTest(Method, ITestContext)}
 * </p>
 */
@Test(dataProvider = "localSessionProvider")
public class StaticStatementTests extends SessionTestSupport {


}
