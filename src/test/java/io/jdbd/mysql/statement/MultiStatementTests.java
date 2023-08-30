package io.jdbd.mysql.statement;

import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.MultiStatement;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.*;

/**
 * <p>
 * This class is the test class of {@link MultiStatement}
 * </p>
 * <p>
 * All test method's session parameter is created by {@link #createLocalSession(ITestNGMethod, ITestContext)},
 * and is closed by {@link #closeSessionAfterTest(Method, ITestContext)}
 * </p>
 */
@Test(dataProvider = "localSessionProvider")
public class MultiStatementTests extends SessionTestSupport {


    /**
     * @see MultiStatement#executeBatchUpdate()
     */
    @Test
    public void executeBatchUpdate(final DatabaseSession session) {
        final MultiStatement statement;
        statement = session.multiStatement();

        statement.addStatement("INSERT mysql_types(my_time,my_time1,my_date,my_datetime,my_datetime6,my_text) VALUES( ? , ? , ? , ? , ? , ? )")

                .bind(0, JdbdType.TIME, LocalTime.now())
                .bind(1, JdbdType.TIME_WITH_TIMEZONE, OffsetTime.now(ZoneOffset.UTC))
                .bind(2, JdbdType.DATE, LocalDate.now())
                .bind(3, JdbdType.TIMESTAMP, LocalDateTime.now())

                .bind(4, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(5, JdbdType.TEXT, "QinArmy's army \\")

                .addStatement("UPDATE mysql_types AS t t.my_datetime = ? AND t.my_decimal = t.my_decimal + ? WHERE t.my_datetime6 < ? LIMIT ?")

                .bind(0, JdbdType.TIMESTAMP, LocalDateTime.now())
                .bind(1, JdbdType.DECIMAL, new BigDecimal("88.66"))
                .bind(2, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(3, JdbdType.INTEGER, 3)

                .addStatement("SELECT t.* FROM mysql_types AS t WHERE t.my_datetime6 < ? AND t.my_decimal < ? LIMIT ? ")

                .bind(0, JdbdType.TIMESTAMP, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(1, JdbdType.DECIMAL, new BigDecimal("88.66"))
                .bind(2, JdbdType.INTEGER, 20);


    }

}
