package io.jdbd.mysql.statement;


import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.statement.BindSingleStatement;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;


/**
 * <p>
 * This class is test class of following :
 *     <ul>
 *         <li>{@link io.jdbd.statement.BindStatement}</li>
 *         <li>{@link io.jdbd.statement.PreparedStatement}</li>
 *     </ul>
 * </p>
 * <p>
 * All test method's session of  statement parameter  is closed by {@link #closeSessionAfterTest(Method, ITestContext)}
 * </p>
 */
public class BindSingleStatementTests extends SessionTestSupport {


    /**
     * @see BindSingleStatement#executeUpdate()
     */
    @Test(invocationCount = 2, dataProvider = "insertDatProvider")
    public final void executeUpdateInsert(final BindSingleStatement statement) {

    }


    @DataProvider(name = "insertDatProvider", parallel = true)
    public final Object[][] insertDatProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String sql;
        sql = "CALL my_random_update_one()";


        final BindSingleStatement statement;
        if ((targetMethod.getCurrentInvocationCount() & 1) == 0) {
            statement = Mono.from(sessionFactory.localSession())
                    .map(session -> session.bindStatement(sql))
                    .block();
        } else {
            statement = Mono.from(sessionFactory.localSession())
                    .flatMap(session -> Mono.from(session.prepareStatement(sql)))
                    .block();
        }

        Assert.assertNotNull(statement);

        final String methodName, key;
        methodName = targetMethod.getMethodName();
        key = targetMethod.getRealClass().getName() + '.' + methodName;

        context.setAttribute(key, statement.getSession());
        return new Object[][]{{statement}};
    }


}
