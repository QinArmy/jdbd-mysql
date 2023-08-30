package io.jdbd.mysql.statement;

import com.alibaba.fastjson2.JSON;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.Statement;
import io.jdbd.statement.StaticStatement;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;


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


    /**
     * @see StaticStatement#executeUpdate(String)
     */
    @Test
    public void executeUpdateInsert(final DatabaseSession session, final String methodName) {

        final StringBuilder builder = new StringBuilder(180);

        final LocalDateTime now = LocalDateTime.now();

        builder.append("INSERT mysql_types(my_datetime,my_datetime6) VALUES(");

        builder.append(Constants.TIMESTAMP_SPACE)
                .append(Constants.QUOTE)
                .append(now.format(MySQLTimes.DATETIME_FORMATTER_6))
                .append(Constants.QUOTE)

                .append(Constants.SPACE_COMMA_SPACE)

                .append(Constants.TIMESTAMP_SPACE)
                .append(Constants.QUOTE)
                .append(now.minusDays(3).format(MySQLTimes.DATETIME_FORMATTER_6))
                .append(Constants.QUOTE)

                .append(')');

        ResultStates resultStates;
        resultStates = Mono.from(session.statement().executeUpdate(builder.toString()))
                .block();

        Assert.assertNotNull(resultStates);

        LOG.debug("{} affectedRows : {}", methodName, resultStates.affectedRows());

    }

    /**
     * @see StaticStatement#executeUpdate(String)
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

        resultStates = Mono.from(session.statement().executeUpdate(procedureSql))
                .onErrorResume(error -> {   // IF NOT EXISTS as of MySQL 8.0.29
                    if (error instanceof ServerException && error.getMessage().contains(procedureName)) {
                        return Mono.empty();
                    }
                    return Mono.error(error);
                })
                .then(Mono.from(session.statement().executeUpdate("call " + procedureName + "()")))
                .block();

        Assert.assertNotNull(resultStates);

        LOG.debug("{} affectedRows : {}", methodName, resultStates.affectedRows());

    }

    /**
     * @see StaticStatement#executeQuery(String, Function, Consumer)
     */
    @Test(dependsOnMethods = "executeUpdateInsert")
    public void executeQuery(final DatabaseSession session, final String methodName) {

        final String sql;
        sql = "SELECT t.* FROM mysql_types AS t LIMIT 20 ";

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);
        final List<? extends Map<String, ?>> mapList;  //TODO 向 idea 提交 泛型 推断 bug,mapList 不需要 <? extends>
        mapList = Flux.from(session.statement().executeQuery(sql, this::mapCurrentRowToMap, statesHolder::set))
                .collectList()
                .block();

        final ResultStates resultStates = statesHolder.get();

        Assert.assertNotNull(mapList);
        Assert.assertNotNull(resultStates);


        final int rowCount, columnCount;
        rowCount = mapList.size();
        if (rowCount == 0) {
            columnCount = 0;
        } else {
            columnCount = mapList.get(0).size();
        }

        Assert.assertEquals(resultStates.rowCount(), rowCount);

        LOG.debug("{} result row count {} , column count {}", methodName, rowCount, columnCount);

        // LOG.debug("row list : \n{}",JSON.toJSONString(mapList));


    }

    /**
     * @see StaticStatement#executeBatchUpdate(List)
     */
    @Test(dependsOnMethods = {"executeUpdateInsert", "executeUpdateCall"})
    public void executeBatchUpdate(final DatabaseSession session) {


        final String sql;
        sql = "CALL my_random_update_one()";

        final int batchItemCount = 2;
        final List<String> sqlList = MySQLCollections.arrayList(batchItemCount);
        for (int i = 0; i < batchItemCount; i++) {
            sqlList.add(sql);
        }

        final List<ResultStates> statesList;
        statesList = Flux.from(session.statement().executeBatchUpdate(sqlList))
                .collectList()
                .block();

        Assert.assertNotNull(statesList);

        Assert.assertEquals(statesList.size(), sqlList.size());

    }

    /**
     * @see StaticStatement#executeBatchQuery(List)
     */
    @Test(dependsOnMethods = "executeUpdateInsert")
    public void executeBatchQuery(final DatabaseSession session, final String methodName) {


        final String sql;
        sql = "SELECT t.* FROM mysql_types AS t LIMIT 20 ";

        final List<String> sqlList = MySQLCollections.arrayList(2);
        sqlList.add(sql);
        sqlList.add(sql);

        QueryResults batchQuery;
        batchQuery = session.statement().executeBatchQuery(sqlList);


        final List<ResultRow> rowList;
        rowList = Flux.from(batchQuery.nextQuery())
                .concatWith(Flux.from(batchQuery.nextQueryFlux())
                        .filter(ResultItem::isRowItem)
                        .map(ResultRow.class::cast)
                )
                .collectList()
                .block();

        Assert.assertNotNull(rowList);

        LOG.debug("{} result row count {} ", methodName, rowList.size());

    }

    /**
     * @see StaticStatement#executeBatchAsMulti(List)
     */
    @Test(dependsOnMethods = {"executeUpdateInsert", "executeUpdateCall"})
    public void executeBatchAsMulti(final DatabaseSession session, final String methodName) {


        final List<String> sqlList = MySQLCollections.arrayList(5);

        sqlList.add("CALL my_random_update_one()");
        sqlList.add("CALL my_random_update_one()");
        sqlList.add("SELECT t.* FROM mysql_types AS t LIMIT 20 ");
        sqlList.add("SELECT t.* FROM mysql_types AS t LIMIT 20 ");

        sqlList.add("SELECT t.* FROM mysql_types AS t LIMIT 20 ");

        final MultiResult multiResult;
        multiResult = session.statement().executeBatchAsMulti(sqlList);

        final List<ResultRow> resultRowList;

        resultRowList = Mono.from(multiResult.nextUpdate())                 // result 1
                .then(Mono.from(multiResult.nextUpdate()))                  // result 2
                .thenMany(multiResult.nextQuery())                          // result 3
                .thenMany(multiResult.nextQuery(CurrentRow::asResultRow))                          // result 4
                .concatWith(Flux.from(multiResult.nextQueryFlux())         // result 5
                        .filter(ResultItem::isRowItem)
                        .map(ResultRow.class::cast)
                ).collectList()
                .block();

        Assert.assertNotNull(resultRowList);

        LOG.debug("{} result row count {} ", methodName, resultRowList.size());
    }

    /**
     * @see StaticStatement#executeBatchAsFlux(List)
     */
    @Test(dependsOnMethods = {"executeUpdateInsert", "executeUpdateCall"})
    public void executeBatchAsFlux(final DatabaseSession session) {


        final List<String> sqlList = MySQLCollections.arrayList(5);

        sqlList.add("CALL my_random_update_one()");
        sqlList.add("CALL my_random_update_one()");
        sqlList.add("SELECT t.* FROM mysql_types AS t LIMIT 20 ");
        sqlList.add("SELECT t.* FROM mysql_types AS t LIMIT 20 ");

        sqlList.add("SELECT t.* FROM mysql_types AS t LIMIT 20 ");

        final List<ResultStates> statesList;

        statesList = Flux.from(session.statement().executeBatchAsFlux(sqlList))
                .filter(ResultItem::isStatesItem)
                .map(ResultStates.class::cast)
                .collectList()
                .block();

        Assert.assertNotNull(statesList);

        Assert.assertFalse(statesList.get(0).hasColumn());
        Assert.assertFalse(statesList.get(1).hasColumn());

        Assert.assertTrue(statesList.get(2).hasColumn());
        Assert.assertTrue(statesList.get(3).hasColumn());
        Assert.assertTrue(statesList.get(4).hasColumn());


    }

    /**
     * @see StaticStatement#executeMultiStmt(String)
     */
    @Test(dependsOnMethods = {"executeUpdateInsert", "executeUpdateCall"})
    public void executeAsFlux(final DatabaseSession session) {

        String multiSql = "CALL my_random_update_one() ; SELECT t.* FROM mysql_types AS t LIMIT 20";

        final List<ResultStates> statesList;
        statesList = Flux.from(session.statement().executeMultiStmt(multiSql))
                .filter(ResultItem::isStatesItem)
                .map(ResultStates.class::cast)
                .collectList()
                .block();

        Assert.assertNotNull(statesList);

        Assert.assertFalse(statesList.get(0).hasColumn());
        Assert.assertTrue(statesList.get(1).hasColumn());

    }

    /**
     * @see Statement#bindStmtVar(String, DataType, Object)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/query-attributes.html">Query Attributes</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY , static statement Query Attributes bind</a>
     */
    @Test
    public void queryWithQueryAttributes(final DatabaseSession session) {
        StaticStatement statement;
        statement = session.statement();

        statement.bindStmtVar("rowNum", JdbdType.INTEGER, 1);

        final String sql;
        sql = "SELECT t.id AS id , CAST(mysql_query_attribute_string('rowNum') AS SIGNED ) AS rowNum FROM mysql_types AS t LIMIT 2 ";

        final List<? extends Map<String, ?>> rowList;

        rowList = Flux.from(statement.executeQuery(sql, this::mapCurrentRowToMap))
                .collectList()
                .block();

        Assert.assertNotNull(rowList);

        LOG.info("queryWithQueryAttributes : \n{}", JSON.toJSONString(rowList));

    }


}
