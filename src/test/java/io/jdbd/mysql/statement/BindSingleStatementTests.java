package io.jdbd.mysql.statement;


import com.alibaba.fastjson2.JSON;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.result.*;
import io.jdbd.statement.BindSingleStatement;
import io.jdbd.statement.OutParameter;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
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
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     * </p>
     *
     * @see BindSingleStatement#executeUpdate()
     */
    @Test(invocationCount = 3, dataProvider = "insertDatProvider")
    public void executeUpdateInsert(final BindSingleStatement statement) {

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

    @Test(invocationCount = 3, dataProvider = "insertDatProvider")
    public void executeBatchUpdateInsert(final BindSingleStatement statement) {
        final int batchItemCount = 800;

        for (int i = 0; i < batchItemCount; i++) {

            statement.bind(0, JdbdType.TIME, LocalTime.now())
                    .bind(1, JdbdType.TIME_WITH_TIMEZONE, OffsetTime.now(ZoneOffset.UTC))
                    .bind(2, JdbdType.DATE, LocalDate.now())
                    .bind(3, JdbdType.TIMESTAMP, LocalDateTime.now())

                    .bind(4, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                    .bind(5, JdbdType.TEXT, "QinArmy's army \\")

                    .addBatch();
        }


        final List<ResultStates> statesList;
        statesList = Flux.from(statement.executeBatchUpdate())
                .collectList()
                .block();

        Assert.assertNotNull(statesList);
        Assert.assertEquals(statesList.size(), batchItemCount);

    }


    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     * </p>
     *
     * @see BindSingleStatement#executeQuery(Function, Consumer)
     */
    @Test(invocationCount = 3, dataProvider = "executeQueryProvider", dependsOnMethods = "executeUpdateInsert")
    public void executeQuery(final BindSingleStatement statement) {

        statement.bind(0, JdbdType.BIGINT, 1)
                .bind(1, JdbdType.TEXT, "%army%")
                .bind(2, JdbdType.TIME, LocalTime.now())
                .bind(3, JdbdType.TIME_WITH_TIMEZONE, OffsetTime.now())

                .bind(4, JdbdType.DATE, LocalDate.now())
                .bind(5, JdbdType.TIMESTAMP, LocalDateTime.now())
                .bind(6, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now())
                .bind(7, JdbdType.INTEGER, 20);

        final List<? extends Map<String, ?>> rowList;

        rowList = Flux.from(statement.executeQuery(this::mapCurrentRowToMap))
                .collectList()
                .block();

        Assert.assertNotNull(rowList);
        Assert.assertTrue(rowList.size() > 0);
        LOG.info("executeQuery row {}", rowList.size());
    }


    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     * </p>
     *
     * @see BindSingleStatement#executeUpdate()
     */
    @Test(invocationCount = 3, dataProvider = "callOutParameterProvider")
    public void executeAsFluxCallOutParameter(final BindSingleStatement statement) {

        statement.bind(0, JdbdType.TIMESTAMP, LocalDateTime.now())
                .bind(1, JdbdType.TIMESTAMP, OutParameter.out("outNow"));

        final List<? extends Map<String, ?>> rowList;

        rowList = Flux.from(statement.executeAsFlux())
                .filter(ResultItem::isRowItem)
                .map(ResultRow.class::cast)
                .map(this::mapCurrentRowToMap)
                .collectList()
                .block();

        Assert.assertNotNull(rowList);
        Assert.assertEquals(rowList.size(), 1);

        LOG.info("executeAsFluxCallOutParameter out parameter : \n{}", JSON.toJSONString(rowList));
    }

    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     * </p>
     *
     * @see BindSingleStatement#executeUpdate()
     */
    @Test(invocationCount = 3, dataProvider = "insertDatProvider")
    public void executeBatchUpdate(final BindSingleStatement statement) {
        final int batchItemCount = 3;

        for (int i = 0; i < batchItemCount; i++) {
            statement.bind(0, JdbdType.TIME, LocalTime.now())
                    .bind(1, JdbdType.TIME_WITH_TIMEZONE, OffsetTime.now(ZoneOffset.UTC))
                    .bind(2, JdbdType.DATE, LocalDate.now())
                    .bind(3, JdbdType.TIMESTAMP, LocalDateTime.now())

                    .bind(4, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                    .bind(5, JdbdType.TEXT, "QinArmy's army \\")

                    .addBatch();

        }

        final List<ResultStates> statesList;
        statesList = Flux.from(statement.executeBatchUpdate())
                .collectList()
                .block();
        Assert.assertNotNull(statesList);

        Assert.assertEquals(statesList.size(), batchItemCount);

    }

    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     * </p>
     *
     * @see BindSingleStatement#executeBatchQuery()
     */
    @Test(invocationCount = 3, dataProvider = "executeQueryProvider", dependsOnMethods = "executeBatchUpdate")
    public void executeBatchQuery(final BindSingleStatement statement) {


        //        way 1:
        //        statement =Mono.from( session.prepareStatement(sql))
        //                .block();
        //        Assert.assertNotNull(statement);
        //
        //        //way 2:
        //        statement = session.bindStatement(sql,true);

        // way 3:
        // statement = session.bindStatement(sql);

        final int batchItemCount = 3;

        for (int i = 0; i < batchItemCount; i++) {

            statement.bind(0, JdbdType.BIGINT, 1)
                    .bind(1, JdbdType.TEXT, "%army%")
                    .bind(2, JdbdType.TIME, LocalTime.now())
                    .bind(3, JdbdType.TIME_WITH_TIMEZONE, OffsetTime.now())

                    .bind(4, JdbdType.DATE, LocalDate.now())
                    .bind(5, JdbdType.TIMESTAMP, LocalDateTime.now())
                    .bind(6, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now())
                    .bind(7, JdbdType.INTEGER, 20)

                    .addBatch();

        }

        final QueryResults batchQuery;
        batchQuery = statement.executeBatchQuery();

        final Function<CurrentRow, Map<String, ?>> function = this::mapCurrentRowToMap;

        final List<Map<String, ?>> rowList;

        rowList = Flux.from(batchQuery.nextQuery(function))
                .concatWith(batchQuery.nextQuery(function))
                .concatWith(batchQuery.nextQuery(function))
                .collectList()
                .block();

        Assert.assertNotNull(rowList);
        Assert.assertTrue(rowList.size() > 0);

    }

    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     * </p>
     *
     * @see BindSingleStatement#executeBatchAsMulti()
     */
    @Test(invocationCount = 3, dataProvider = "callOutParameterProvider")
    public void executeBatchAsMulti(final BindSingleStatement statement) {

        final int batchItemCount = 3;
        for (int i = 0; i < batchItemCount; i++) {
            statement.bind(0, JdbdType.TIMESTAMP, LocalDateTime.now())
                    .bind(1, JdbdType.TIMESTAMP, OutParameter.out("outNow"))

                    .addBatch();
        }

        // each batch item , MySQL server will produce tow result,
        // result 1 : out parameter result set
        // result 2 : the result of CALL command
        final MultiResult multiResult;
        multiResult = statement.executeBatchAsMulti();

        // here, don't need thread safe list,because main thread blocking.
        final List<Map<String, ?>> outParamMapList = new ArrayList<>(batchItemCount);

        // here, don't need thread safe list,because main thread blocking.
        final List<ResultStates> statesList = new ArrayList<>(batchItemCount);

        // following first batch item results
        Flux.from(multiResult.nextQuery(this::mapCurrentRowToMap))
                .last()
                .doOnSuccess(outParamMapList::add) // here ,due to main thread blocking, so current thread in EventLoop.
                .then(Mono.from(multiResult.nextUpdate()))
                .doOnSuccess(statesList::add) // here ,due to main thread blocking, so  current thread in EventLoop.

                // following second batch item results
                .thenMany(multiResult.nextQuery(this::mapCurrentRowToMap))
                .last()
                .doOnSuccess(outParamMapList::add) // here ,due to main thread blocking, so current thread in EventLoop.
                .then(Mono.from(multiResult.nextUpdate()))
                .doOnSuccess(statesList::add)// here ,due to main thread blocking, so  current thread in EventLoop.

                // following third batch item results
                .thenMany(multiResult.nextQuery(this::mapCurrentRowToMap))
                .last()
                .doOnSuccess(outParamMapList::add) // here ,due to main thread blocking, so current thread in EventLoop.
                .then(Mono.from(multiResult.nextUpdate()))
                .doOnSuccess(statesList::add)// here ,due to main thread blocking, so  current thread in EventLoop.

                .block();


        Assert.assertEquals(outParamMapList.size(), batchItemCount);
        Assert.assertEquals(statesList.size(), batchItemCount);

        LOG.info("executeBatchAsMulti  outParamMapList : \n{}", outParamMapList);

    }

    /**
     * <p>
     * Test :
     *     <ul>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#bindStatement(String, boolean)}</li>
     *         <li>{@link io.jdbd.session.DatabaseSession#prepareStatement(String)}</li>
     *     </ul>
     * </p>
     *
     * @see BindSingleStatement#executeBatchAsMulti()
     */
    @Test(invocationCount = 3, dataProvider = "callOutParameterProvider")
    public void executeBatchAsFlux(final BindSingleStatement statement) {

        final int batchItemCount = 3;

        for (int i = 0; i < batchItemCount; i++) {
            statement.bind(0, JdbdType.TIMESTAMP, LocalDateTime.now())
                    .bind(1, JdbdType.TIMESTAMP, OutParameter.out("outNow"))

                    .addBatch();
        }


        final List<? extends Map<String, ?>> rowList;

        // each batch item , MySQL server will produce tow result,
        // result 1 : out parameter result set , just one row.
        // result 2 : the result of CALL command
        rowList = Flux.from(statement.executeBatchAsFlux())
                .filter(ResultItem::isRowItem)
                .map(ResultRow.class::cast)
                .map(this::mapCurrentRowToMap)
                .collectList()
                .block();

        Assert.assertNotNull(rowList);
        Assert.assertEquals(rowList.size(), batchItemCount);

        LOG.info("executeBatchAsFlux out parameter : \n{}", rowList);

    }

    /**
     * @see io.jdbd.statement.Statement#bindStmtVar(String, DataType, Object)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/query-attributes.html">Query Attributes</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY , static statement Query Attributes bind</a>
     */
    @Test(invocationCount = 3, dataProvider = "queryAttrProvider")
    public void queryWithQueryAttributes(final BindSingleStatement statement) {

        statement.bindStmtVar("rowNum", JdbdType.INTEGER, 1);

        statement.bind(0, JdbdType.TIMESTAMP, LocalDateTime.now())
                .bind(1, JdbdType.INTEGER, 2);

        final List<? extends Map<String, ?>> rowList;

        rowList = Flux.from(statement.executeQuery(this::mapCurrentRowToMap))
                .collectList()
                .block();

        Assert.assertNotNull(rowList);

        LOG.info("queryWithQueryAttributes : \n{}", JSON.toJSONString(rowList));

    }

    /**
     * @see io.jdbd.statement.Statement#setFetchSize(int)
     */
    @Test(invocationCount = 3, dataProvider = "simpleQueryProvider", dependsOnMethods = "executeBatchUpdateInsert")
    public void queryWithFetch(final BindSingleStatement statement) {
        statement.bind(0, JdbdType.INTEGER, 20)
                .setFetchSize(5);

        final Long rowCount;
        rowCount = Flux.from(statement.executeQuery(row -> Optional.empty()))
                .count()
                .block();

        Assert.assertNotNull(rowCount);
        Assert.assertTrue(rowCount > 0);

        LOG.info("queryWithFetch row :{}", rowCount);
    }

    @DataProvider(name = "queryAttrProvider", parallel = true)
    public final Object[][] queryAttrProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String sql;
        sql = "SELECT t.id AS id , CAST(mysql_query_attribute_string('rowNum') AS SIGNED ) AS rowNum FROM mysql_types AS t WHERE t.my_datetime < ? LIMIT ? ";
        final BindSingleStatement statement;
        statement = createSingleStatement(targetMethod, context, sql);

        return new Object[][]{{statement}};
    }


    @DataProvider(name = "insertDatProvider", parallel = true)
    public final Object[][] insertDatProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String sql = "INSERT mysql_types(my_time,my_time1,my_date,my_datetime,my_datetime6,my_text) VALUES( ? , ? , ? , ? , ? , ? )";

        final BindSingleStatement statement;
        statement = createSingleStatement(targetMethod, context, sql);

        return new Object[][]{{statement}};
    }

    @DataProvider(name = "callOutParameterProvider", parallel = true)
    public final Object[][] callOutParameterProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String procedureSql, sql;
        procedureSql = "CREATE PROCEDURE army_inout_out_now(INOUT inoutNow DATETIME, OUT outNow DATETIME)\n" +
                "    LANGUAGE SQL\n" +
                "BEGIN\n" +
                "    SELECT current_timestamp(3), DATE_ADD(inoutNow, INTERVAL 1 DAY) INTO outNow,inoutNow;\n" +
                "END";

        sql = "CALL army_inout_out_now( ? , ?)";


        final BindSingleStatement statement;
        statement = createSingleStatement(targetMethod, context, sql);

        if (targetMethod.getCurrentInvocationCount() == 0) {
            Mono.from(statement.getSession().executeUpdate(procedureSql))
                    .onErrorResume(error -> {
                        if (error instanceof ServerException && error.getMessage().contains("army_inout_out_now")) {
                            return Mono.empty();
                        }
                        return Mono.error(error);
                    })
                    .block();
        }
        return new Object[][]{{statement}};
    }


    /**
     * @see #executeQuery(BindSingleStatement)
     */
    @DataProvider(name = "executeQueryProvider", parallel = true)
    public final Object[][] executeQueryProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String sql = "SELECT t.* FROM mysql_types AS t WHERE t.id > ? AND t.my_text LIKE ? " +
                "AND t.my_time < ? AND t.my_time1 < ? AND t.my_date = ? AND t.my_datetime < ? " +
                "AND t.my_datetime6 < ? LIMIT ? ";

        final BindSingleStatement statement;
        statement = createSingleStatement(targetMethod, context, sql);
        return new Object[][]{{statement}};
    }

    @DataProvider(name = "simpleQueryProvider", parallel = true)
    public final Object[][] simpleQueryProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String sql = "SELECT t.* FROM mysql_types AS t LIMIT ?";
        final BindSingleStatement statement;
        statement = createSingleStatement(targetMethod, context, sql);
        return new Object[][]{{statement}};
    }


}
