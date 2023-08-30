package io.jdbd.mysql.statement;

import com.alibaba.fastjson2.JSON;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.MultiStatement;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

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

                .addStatement("UPDATE mysql_types AS t SET t.my_datetime = ? , t.my_decimal = t.my_decimal + ? WHERE t.my_datetime6 < ? LIMIT ?")

                .bind(0, JdbdType.TIMESTAMP, LocalDateTime.now())
                .bind(1, JdbdType.DECIMAL, new BigDecimal("88.66"))
                .bind(2, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(3, JdbdType.INTEGER, 3);


        final List<ResultStates> statesList;

        statesList = Flux.from(statement.executeBatchUpdate())
                .collectList()
                .block();

        Assert.assertNotNull(statesList);
        Assert.assertEquals(statesList.size(), 2);

    }

    /**
     * @see MultiStatement#executeBatchQuery()
     */
    @Test
    public void executeBatchQuery(final DatabaseSession session) {
        final MultiStatement statement;
        statement = session.multiStatement();

        statement.addStatement("SELECT t.* FROM mysql_types AS t WHERE t.my_datetime6 < ? AND t.my_decimal < ? LIMIT ? ")

                .bind(0, JdbdType.TIMESTAMP, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(1, JdbdType.DECIMAL, new BigDecimal("88.66"))
                .bind(2, JdbdType.INTEGER, 20)

                .addStatement("SELECT t.* FROM mysql_types AS t WHERE t.my_datetime6 < ? AND t.my_decimal < ? LIMIT ? ")

                .bind(0, JdbdType.TIMESTAMP, LocalDateTime.now().minusDays(2))
                .bind(1, JdbdType.DECIMAL, new BigDecimal("999999.66"))
                .bind(2, JdbdType.INTEGER, 10);

        final QueryResults queryResults;
        queryResults = statement.executeBatchQuery();

        final Function<CurrentRow, Map<String, ?>> function = this::mapCurrentRowToMap;

        final List<Map<String, ?>> rowList;

        rowList = Flux.from(queryResults.nextQuery(function))
                .concatWith(queryResults.nextQuery(function))
                .collectList()
                .block();

        Assert.assertNotNull(rowList);

    }


    /**
     * @see MultiStatement#executeBatchUpdate()
     */
    @Test
    public void executeBatchAsMulti(final DatabaseSession session) {
        final MultiStatement statement;
        statement = session.multiStatement();

        statement.addStatement("INSERT mysql_types(my_time,my_time1,my_date,my_datetime,my_datetime6,my_text) VALUES( ? , ? , ? , ? , ? , ? )")

                .bind(0, JdbdType.TIME, LocalTime.now())
                .bind(1, JdbdType.TIME_WITH_TIMEZONE, OffsetTime.now(ZoneOffset.UTC))
                .bind(2, JdbdType.DATE, LocalDate.now())
                .bind(3, JdbdType.TIMESTAMP, LocalDateTime.now())

                .bind(4, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(5, JdbdType.TEXT, "QinArmy's army \\")

                .addStatement("UPDATE mysql_types AS t SET t.my_datetime = ? , t.my_decimal = t.my_decimal + ? WHERE t.my_datetime6 < ? LIMIT ?")

                .bind(0, JdbdType.TIMESTAMP, LocalDateTime.now())
                .bind(1, JdbdType.DECIMAL, new BigDecimal("88.66"))
                .bind(2, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(3, JdbdType.INTEGER, 3)

                .addStatement("SELECT t.* FROM mysql_types AS t WHERE t.my_datetime6 < ? AND t.my_decimal < ? LIMIT ? ")

                .bind(0, JdbdType.TIMESTAMP, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(1, JdbdType.DECIMAL, new BigDecimal("88.66"))
                .bind(2, JdbdType.INTEGER, 20);


        final MultiResult multiResult;
        multiResult = statement.executeBatchAsMulti();


        Mono.from(multiResult.nextUpdate())
                .then(Mono.from(multiResult.nextUpdate()))
                .thenMany(multiResult.nextQuery(this::mapCurrentRowToMap))
                .collectList()
                .block();

    }

    @Test
    public void executeBatchAsFlux(final DatabaseSession session) {
        final MultiStatement statement;
        statement = session.multiStatement();

        statement.addStatement("INSERT mysql_types(my_time,my_time1,my_date,my_datetime,my_datetime6,my_text) VALUES( ? , ? , ? , ? , ? , ? )")

                .bind(0, JdbdType.TIME, LocalTime.now())
                .bind(1, JdbdType.TIME_WITH_TIMEZONE, OffsetTime.now(ZoneOffset.UTC))
                .bind(2, JdbdType.DATE, LocalDate.now())
                .bind(3, JdbdType.TIMESTAMP, LocalDateTime.now())

                .bind(4, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(5, JdbdType.TEXT, "QinArmy's army \\")

                .addStatement("UPDATE mysql_types AS t SET t.my_datetime = ? , t.my_decimal = t.my_decimal + ? WHERE t.my_datetime6 < ? LIMIT ?")

                .bind(0, JdbdType.TIMESTAMP, LocalDateTime.now())
                .bind(1, JdbdType.DECIMAL, new BigDecimal("88.66"))
                .bind(2, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(3, JdbdType.INTEGER, 3)

                .addStatement("SELECT t.* FROM mysql_types AS t WHERE t.my_datetime6 < ? AND t.my_decimal < ? LIMIT ? ")

                .bind(0, JdbdType.TIMESTAMP, OffsetDateTime.now(ZoneOffset.UTC))
                .bind(1, JdbdType.DECIMAL, new BigDecimal("88.66"))
                .bind(2, JdbdType.INTEGER, 20);


        final AtomicReference<ResultStates> insertStatesHolder = new AtomicReference<>(null);

        final AtomicReference<ResultStates> updateStatesHolder = new AtomicReference<>(null);

        final List<? extends Map<String, ?>> rowList;

        rowList = Flux.from(statement.executeBatchAsFlux())
                .filter(ResultItem::isRowOrStatesItem)
                .doOnNext(item -> {
                    switch (item.getResultNo()) {
                        case 1:
                            insertStatesHolder.set((ResultStates) item);
                            break;
                        case 2:
                            updateStatesHolder.set((ResultStates) item);
                            break;
                        default:
                            //no-op
                    }
                })
                .filter(ResultItem::isRowItem)
                .map(ResultRow.class::cast)
                .map(this::mapCurrentRowToMap)
                .collectList()
                .block();

        Assert.assertNotNull(insertStatesHolder.get());
        Assert.assertNotNull(updateStatesHolder.get());
        Assert.assertNotNull(rowList);


    }


    /**
     * @see io.jdbd.statement.Statement#bindStmtVar(String, DataType, Object)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/query-attributes.html">Query Attributes</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY , static statement Query Attributes bind</a>
     */
    @Test
    public void queryWithQueryAttributes(final DatabaseSession session) {
        final MultiStatement statement;
        statement = session.multiStatement();

        statement.bindStmtVar("rowNum", JdbdType.INTEGER, 1);

        final String sql;
        sql = "SELECT t.id AS id , CAST(mysql_query_attribute_string('rowNum') AS SIGNED ) AS rowNum FROM mysql_types AS t WHERE t.my_datetime < ? LIMIT ? ";

        statement.addStatement(sql)
                .bind(0, JdbdType.TIMESTAMP, LocalDateTime.now())
                .bind(1, JdbdType.INTEGER, 2)

                .addStatement(sql)
                .bind(0, JdbdType.TIMESTAMP, LocalDateTime.now().minusDays(2))
                .bind(1, JdbdType.INTEGER, 1)

                .addStatement(sql)
                .bind(0, JdbdType.TIMESTAMP, LocalDateTime.now().plusDays(2))
                .bind(1, JdbdType.INTEGER, 3);

        final List<? extends Map<String, ?>> rowList;

        rowList = Flux.from(statement.executeBatchAsFlux())
                .filter(ResultItem::isRowItem)
                .map(ResultRow.class::cast)
                .map(this::mapCurrentRowToMap)
                .collectList()
                .block();

        Assert.assertNotNull(rowList);

        LOG.info("queryWithQueryAttributes : \n{}", JSON.toJSONString(rowList));

    }


}
