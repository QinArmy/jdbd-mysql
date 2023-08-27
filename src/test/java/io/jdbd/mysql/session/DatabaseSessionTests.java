package io.jdbd.mysql.session;


import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.ResultStates;
import io.jdbd.result.ServerException;
import io.jdbd.session.DatabaseSession;
import org.testng.Assert;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * <p>
 * This class is the test class of {@link MySQLDatabaseSession}
 * </p>
 */
@Test(dataProvider = "localSessionProvider")
public class DatabaseSessionTests extends SessionTestSupport {


    /**
     * @see DatabaseSession#executeUpdate(String)
     */
    @Test
    public void executeUpdateInsert(final DatabaseSession session, final String methodName) {
        final StringBuilder builder = new StringBuilder(180);

        final LocalDateTime now = LocalDateTime.now();

        builder.append("INSERT mysql_types(my_datetime,my_datetime6) VALUES(");

        builder.append(Constants.QUOTE)
                .append(now.format(MySQLTimes.DATETIME_FORMATTER_6))
                .append(Constants.QUOTE)
                .append(Constants.SPACE_COMMA_SPACE)
                .append(Constants.QUOTE)
                .append(now.minusDays(3).format(MySQLTimes.DATETIME_FORMATTER_6))
                .append(Constants.QUOTE)
                .append(')');

        ResultStates resultStates;
        resultStates = Mono.from(session.executeUpdate(builder.toString()))
                .block();

        Assert.assertNotNull(resultStates);

        LOG.info("{} affectedRows : {}", methodName, resultStates.affectedRows());

    }

    /**
     * @see DatabaseSession#executeUpdate(String)
     */
    @Test(dependsOnMethods = "executeUpdateInsert")
    public void executeUpdateCall(final DatabaseSession session, final String methodName) {
        final String procedureName, procedureSql;
        procedureName = "my_random_update_one";
        procedureSql = "CREATE PROCEDURE " + procedureName + "()\n" +
                "    LANGUAGE SQL\n" +
                "BEGIN\n" +
                "    DECLARE myRandomId BIGINT DEFAULT 1;\n" +
                "    SELECT t.id FROM mysql_types AS t LIMIT 1 INTO myRandomId;\n" +
                "    UPDATE mysql_types AS t SET t.my_datetime = current_timestamp(0) WHERE t.id = myRandomId;\n" +
                "END";


        ResultStates resultStates;

        resultStates = Mono.from(session.executeUpdate(procedureSql))
                .onErrorResume(error -> {   // IF NOT EXISTS as of MySQL 8.0.29
                    if (error instanceof ServerException && error.getMessage().contains(procedureName)) {
                        return Mono.empty();
                    }
                    return Mono.error(error);
                })
                .then(Mono.from(session.executeUpdate("call " + procedureName + "()")))
                .block();

        Assert.assertNotNull(resultStates);

        LOG.info("{} affectedRows : {}", methodName, resultStates.affectedRows());

    }

}
