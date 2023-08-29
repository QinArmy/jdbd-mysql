package io.jdbd.mysql.statement;


import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.BindSingleStatement;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.time.*;
import java.util.function.Function;


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

        statement.bind(0, JdbdType.TIME, LocalTime.now())
                .bind(1, JdbdType.TIME_WITH_TIMEZONE, OffsetTime.now(ZoneOffset.UTC))
                .bind(2, JdbdType.DATE, LocalDate.now())
                .bind(3, JdbdType.TIMESTAMP, LocalDateTime.now())

                .bind(4, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(5, JdbdType.TEXT, "QinArmy's army \\");

        final ResultStates resultStates;
        resultStates = Mono.from(statement.executeUpdate())
                .block();

        Assert.assertNotNull(resultStates);
        Assert.assertFalse(resultStates.hasColumn());
        Assert.assertEquals(resultStates.affectedRows(), 1L);

    }


    @DataProvider(name = "insertDatProvider", parallel = true)
    public final Object[][] insertDatProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String sql;
        sql = "INSERT mysql_types(my_time,my_time1,my_date,my_datetime,my_datetime6,my_text) VALUES( ? , ? , ? , ? , ? , ? )";


        final Function<DatabaseSession, BindSingleStatement> function;
        function = cacheSession -> {
            final BindSingleStatement statement;
            if ((targetMethod.getCurrentInvocationCount() & 1) == 0) {
                statement = Mono.justOrEmpty(cacheSession)
                        .switchIfEmpty(Mono.from(sessionFactory.localSession()))
                        .map(session -> session.bindStatement(sql))
                        .block();
            } else {
                statement = Mono.justOrEmpty(cacheSession)
                        .switchIfEmpty(Mono.from(sessionFactory.localSession()))
                        .flatMap(session -> Mono.from(session.prepareStatement(sql)))
                        .block();
            }

            Assert.assertNotNull(statement);

            return statement;
        };


        final BindSingleStatement statement;
        statement = invokeDataProvider(targetMethod, context, function);

        return new Object[][]{{statement}};
    }


}
