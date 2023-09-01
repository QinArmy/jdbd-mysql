package io.jdbd.mysql.session;


import io.jdbd.JdbdException;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.*;
import io.jdbd.session.*;
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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This class is the test class of {@link MySQLDatabaseSession}
 * </p>
 * <p>
 * All test method's session parameter is created by {@link #createLocalSession(ITestNGMethod, ITestContext)},
 * and is closed by {@link #closeSessionAfterTest(Method, ITestContext)}
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
        resultStates = Mono.from(session.executeUpdate(builder.toString()))
                .block();

        Assert.assertNotNull(resultStates);

        LOG.debug("{} affectedRows : {}", methodName, resultStates.affectedRows());

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

        LOG.debug("{} affectedRows : {}", methodName, resultStates.affectedRows());

    }

    /**
     * @see DatabaseSession#executeQuery(String, Function, Consumer)
     */
    @Test(dependsOnMethods = "executeUpdateInsert")
    public void executeQuery(final DatabaseSession session, final String methodName) {
        final String sql;
        sql = "SELECT t.* FROM mysql_types AS t LIMIT 20 ";

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);
        final List<? extends Map<String, ?>> mapList;  //TODO 向 idea 提交 泛型 推断 bug,mapList 不需要 <? extends>
        mapList = Flux.from(session.executeQuery(sql, this::mapCurrentRowToMap, statesHolder::set))
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
     * @see DatabaseSession#executeBatchUpdate(List)
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
        statesList = Flux.from(session.executeBatchUpdate(sqlList))
                .collectList()
                .block();

        Assert.assertNotNull(statesList);

        Assert.assertEquals(statesList.size(), sqlList.size());

    }

    /**
     * @see DatabaseSession#executeBatchQuery(List)
     */
    @Test(dependsOnMethods = "executeUpdateInsert")
    public void executeBatchQuery(final DatabaseSession session, final String methodName) {
        final String sql;
        sql = "SELECT t.* FROM mysql_types AS t LIMIT 20 ";

        final List<String> sqlList = MySQLCollections.arrayList(2);
        sqlList.add(sql);
        sqlList.add(sql);

        QueryResults batchQuery;
        batchQuery = session.executeBatchQuery(sqlList);


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
     * @see DatabaseSession#executeBatchAsMulti(List)
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
        multiResult = session.executeBatchAsMulti(sqlList);

        final List<ResultRow> resultRowList;

        resultRowList = Mono.from(multiResult.nextUpdate())                 // result 1
                .then(Mono.from(multiResult.nextUpdate()))                  // result 2
                .thenMany(multiResult.nextQuery())                          // result 3
                .thenMany(multiResult.nextQuery(CurrentRow.AS_RESULT_ROW))                          // result 4
                .concatWith(Flux.from(multiResult.nextQueryFlux())         // result 5
                        .filter(ResultItem::isRowItem)
                        .map(ResultRow.class::cast)
                ).collectList()
                .block();

        Assert.assertNotNull(resultRowList);

        LOG.debug("{} result row count {} ", methodName, resultRowList.size());
    }

    /**
     * @see DatabaseSession#executeBatchAsFlux(List)
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

        statesList = Flux.from(session.executeBatchAsFlux(sqlList))
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
     * @see DatabaseSession#executeMultiStmt(String)
     */
    @Test(dependsOnMethods = {"executeUpdateInsert", "executeUpdateCall"})
    public void executeAsFlux(final DatabaseSession session) {
        String multiSql = "CALL my_random_update_one() ; SELECT t.* FROM mysql_types AS t LIMIT 20";

        final List<ResultStates> statesList;
        statesList = Flux.from(session.executeMultiStmt(multiSql))
                .filter(ResultItem::isStatesItem)
                .map(ResultStates.class::cast)
                .collectList()
                .block();

        Assert.assertNotNull(statesList);

        Assert.assertFalse(statesList.get(0).hasColumn());
        Assert.assertTrue(statesList.get(1).hasColumn());

    }

    /**
     * @see DatabaseSession#inTransaction()
     */
    @Test
    public void inTransaction(final LocalDatabaseSession session) {
        final Boolean match;
        match = Mono.from(session.startTransaction(TransactionOption.option(Isolation.READ_COMMITTED, false)))
                .flatMap(s -> {
                    Mono<LocalDatabaseSession> mono;
                    if (s.inTransaction()) {
                        mono = Mono.from(s.commit());
                    } else {
                        mono = Mono.error(new RuntimeException("transaction start bug"));
                    }
                    return mono;
                }).flatMap(s -> Mono.from(s.commit()))
                .flatMap(s -> {
                    Mono<LocalDatabaseSession> mono;
                    if (s.inTransaction()) {
                        mono = Mono.error(new RuntimeException("transaction commit bug"));
                    } else {
                        mono = Mono.just(s);
                    }
                    return mono;
                }).map(s -> Objects.equals(s, session))
                .block();

        Assert.assertEquals(match, Boolean.TRUE);
    }

    /**
     * @see DatabaseSession#transactionStatus()
     */
    @Test
    public void transactionStatus(final LocalDatabaseSession session, final String methodName) {

        Mono.from(session.startTransaction(TransactionOption.option(Isolation.READ_COMMITTED, false)))
                .flatMap(s -> Mono.from(s.transactionStatus()))
                .doOnSuccess(status -> {
                    Assert.assertTrue(status.inTransaction());
                    Assert.assertFalse(status.isReadOnly());
                    Assert.assertEquals(status.isolation(), Isolation.READ_COMMITTED);
                })
                .then(Mono.from(session.commit())) // driver don't send message to database server before subscribing.
                .flatMap(s -> Mono.from(s.transactionStatus()))
                .doOnSuccess(status -> {
                    Assert.assertFalse(status.inTransaction());
                    LOG.debug("{} session isolation : {} , session read only : {}", methodName, status.isolation().name(), status.isReadOnly());
                })
                .block();

    }

    /**
     * @see DatabaseSession#setTransactionCharacteristics(TransactionOption)
     */
    @Test
    public void setTransactionCharacteristics(final DatabaseSession session) {
        TransactionOption txOption;

        TransactionStatus txStatus;

        txOption = TransactionOption.option(Isolation.READ_COMMITTED, true);
        txStatus = Mono.from(session.setTransactionCharacteristics(txOption))
                .flatMap(s -> Mono.from(s.transactionStatus()))
                .block();

        Assert.assertNotNull(txStatus);
        Assert.assertFalse(txStatus.inTransaction());
        Assert.assertEquals(txStatus.isolation(), Isolation.READ_COMMITTED);
        Assert.assertTrue(txStatus.isReadOnly());


        txOption = TransactionOption.option(null, true);
        txStatus = Mono.from(session.setTransactionCharacteristics(txOption))
                .flatMap(s -> Mono.from(s.transactionStatus()))
                .block();

        Assert.assertNotNull(txStatus);
        Assert.assertFalse(txStatus.inTransaction());
        Assert.assertNotNull(txStatus.isolation());
        Assert.assertTrue(txStatus.isReadOnly());

    }

    /**
     * @see DatabaseSession#setSavePoint(String, Function)
     * @see DatabaseSession#releaseSavePoint(SavePoint, Function)
     * @see DatabaseSession#rollbackToSavePoint(SavePoint, Function)
     */
    @Test(dependsOnMethods = "executeUpdateInsert")
    public void savePoint(final LocalDatabaseSession session) {
        final String updateSql;
        updateSql = "UPDATE mysql_types AS t SET t.my_decimal = t.my_decimal + 888.66 WHERE t.my_datetime < current_timestamp LIMIT 1 ";


        Mono.from(session.startTransaction())
                .flatMap(s -> Mono.from(s.executeUpdate(updateSql)))

                .flatMap(s -> Mono.from(session.setSavePoint()))
                .flatMap(s -> Mono.from(session.releaseSavePoint(s)))

                .flatMap(s -> Mono.from(session.setSavePoint()))
                .flatMap(s -> Mono.from(session.rollbackToSavePoint(s)))

                .flatMap(s -> Mono.from(session.setSavePoint("s1")))
                .flatMap(s -> Mono.from(session.releaseSavePoint(s)))

                .flatMap(s -> Mono.from(session.setSavePoint("s2")))
                .flatMap(s -> Mono.from(session.rollbackToSavePoint(s)))

                .flatMap(s -> Mono.from(session.setSavePoint("army's point")))
                .flatMap(s -> Mono.from(session.releaseSavePoint(s)))

                .flatMap(s -> Mono.from(session.setSavePoint("888888886666666")))
                .flatMap(s -> Mono.from(session.rollbackToSavePoint(s, option -> null)))

                .flatMap(s -> Mono.from(s.commit()))
                .block();

    }

    /**
     * @see DatabaseSession#setSavePoint(String, Function)
     */
    @Test(dependsOnMethods = "executeUpdateInsert", expectedExceptions = JdbdException.class)
    public void savePointContainBacktick(final LocalDatabaseSession session) {

        final String updateSql;
        updateSql = "UPDATE mysql_types AS t SET t.my_decimal = t.my_decimal + 888.66 WHERE t.my_datetime < current_timestamp LIMIT 1 ";

        Mono.from(session.startTransaction())
                .flatMap(s -> Mono.from(s.executeUpdate(updateSql)))
                .onErrorMap(RuntimeException::new)
                .flatMap(s -> Mono.from(session.setSavePoint("s1`", option -> null)))
                .block();

    }


}
