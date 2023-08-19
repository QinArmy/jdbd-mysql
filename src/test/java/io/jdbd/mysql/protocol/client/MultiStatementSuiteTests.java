package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Groups;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.session.SessionAdjutant;
import io.jdbd.result.MultiResult;
import io.jdbd.result.NoMoreResultException;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.ResultType;
import io.jdbd.vendor.SubscribeException;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.Stmts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.*;


/**
 * <p>
 * id range [260,299]
 * </p>
 *
 * @see ComQueryTask#multiStmtBatch(BindMultiStmt, TaskAdjutant)
 * @see ComQueryTask#multiStmtAsMulti(BindMultiStmt, TaskAdjutant)
 * @see ComQueryTask#multiStmtAsFlux(BindMultiStmt, TaskAdjutant)
 * @see ComQueryTask#bindBatch(BindBatchStmt, TaskAdjutant)
 */
@Test(groups = {Groups.MULTI_STMT}, dependsOnGroups = {Groups.COM_QUERY, Groups.DATA_PREPARE})
public class MultiStatementSuiteTests extends AbstractTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(MultiStatementSuiteTests.class);

    static final Queue<TaskAdjutant> MULTI_STMT_TASK_ADJUTANT_QUEUE = new LinkedBlockingQueue<>();

    private static final SessionAdjutant MULTI_STMT_SESSION_ADJUTANT = createMultiStmtSessionAdjutant();

    private final int ROW_COUNT = 10;

    @AfterClass
    public void afterClass() {
        Flux.fromIterable(MULTI_STMT_TASK_ADJUTANT_QUEUE)
                .flatMap(QuitTask::quit)
                .then()
                .block();

        MULTI_STMT_TASK_ADJUTANT_QUEUE.clear();
    }

    /**
     * @see ComQueryTask#batchAsMulti(StaticBatchStmt, TaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void batchAsMulti() throws Throwable {
        LOG.info("batchAsMulti test start");
        final TaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();

        String sql;
        List<String> sqlList = new ArrayList<>(6);

        sql = "UPDATE mysql_types as t SET t.name = 'mysql' WHERE t.id = 260";//[1] update
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_date = '%s' WHERE t.id = 261", LocalDate.now());//[2] update
        sqlList.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 260 ORDER BY t.id LIMIT %s", ROW_COUNT);// [3] query
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = 262", LocalTime.now());// [4] update
        sqlList.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 260 ORDER BY t.id LIMIT %s", ROW_COUNT); //[5] query
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = 263", LocalTime.now());//[6] update
        sqlList.add(sql);

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        //below defer serially subscribe
        final MultiResult multiResults1 = ComQueryTask.batchAsMulti(Stmts.batch(sqlList), adjutant);
        Mono.from(multiResults1.nextUpdate())//1. immediately subscribe update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .then(Mono.from(multiResults1.nextUpdate()))// 2.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .thenMany(multiResults1.nextQuery())// 3.defer subscribe  query
                .switchIfEmpty(emptyError())
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                    statesHolder.set(null);
                })

                .then(Mono.from(multiResults1.nextUpdate()))// 4.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .thenMany(multiResults1.nextQuery())// 5.defer subscribe  query
                .switchIfEmpty(emptyError())
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "5.defer subscribe  query states.");
                    statesHolder.set(null);
                })

                .then(Mono.from(multiResults1.nextUpdate()))// 6.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .block();

        //below immediately serially subscribe
        final MultiResult multiResults2;
        multiResults2 = ComQueryTask.batchAsMulti(Stmts.batch(sqlList), adjutant);

        final AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

        Mono.from(multiResults2.nextUpdate())
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .subscribe();// 1. immediately subscribe [1] update

        Mono.from(multiResults2.nextUpdate())
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .subscribe();//2. immediately subscribe [2] update


        Flux.from(multiResults2.nextQuery())
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.immediately subscribe  query states.");
                    statesHolder.set(null);
                })
                .subscribe();//3. immediately subscribe [3] query

        Mono.from(multiResults2.nextUpdate())
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .subscribe();//4. immediately subscribe [4] update

        Flux.from(multiResults2.nextQuery())
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.immediately subscribe  query states.");
                    statesHolder.set(null);
                })
                .subscribe();//5. immediately subscribe [5] query

        Mono.from(multiResults2.nextUpdate())
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)
                .block();//6. immediately subscribe [6] update

        final Throwable error = errorHolder.get();
        if (error != null) {
            throw error;
        }
        LOG.info("multiStatement test success");
        releaseMultiStmtConnection(adjutant);
    }


    /**
     * @see ComQueryTask#multiStmtAsMulti(BindMultiStmt, TaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void multiStmtAsMulti() throws Throwable {
        LOG.info("multiStatement test start");
        final TaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();

        String sql;
        final List<BindStmt> stmtGroup = new ArrayList<>(6);

        sql = "UPDATE mysql_types as t SET t.name = 'mysql' WHERE t.id = ?";//[1] update
        stmtGroup.add(Stmts.single(sql, MySQLType.BIGINT, 264));

        sql = String.format("UPDATE mysql_types as t SET t.my_date = '%s' WHERE t.id = ?", LocalDate.now());//[2] update
        stmtGroup.add(Stmts.single(sql, MySQLType.BIGINT, 265));

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > ? ORDER BY t.id LIMIT %s", ROW_COUNT);// [3] query
        stmtGroup.add(Stmts.single(sql, MySQLType.BIGINT, 260));

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = ?", LocalTime.now());// [4] update
        stmtGroup.add(Stmts.single(sql, MySQLType.BIGINT, 266));

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > ? ORDER BY t.id LIMIT %s", ROW_COUNT); //[5] query
        stmtGroup.add(Stmts.single(sql, MySQLType.BIGINT, 260));

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = ?", LocalTime.now());//[6] update
        stmtGroup.add(Stmts.single(sql, MySQLType.BIGINT, 267));

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        //below defer serially subscribe
        final MultiResult multiResults1 = ComQueryTask.multiStmtAsMulti(Stmts.multi(stmtGroup), adjutant);
        Mono.from(multiResults1.nextUpdate())//1. immediately subscribe update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .then(Mono.from(multiResults1.nextUpdate()))// 2.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .thenMany(multiResults1.nextQuery())// 3.defer subscribe  query
                .switchIfEmpty(emptyError())
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                    statesHolder.set(null);
                })

                .then(Mono.from(multiResults1.nextUpdate()))// 4.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .thenMany(multiResults1.nextQuery())// 5.defer subscribe  query
                .switchIfEmpty(emptyError())
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                    statesHolder.set(null);
                })

                .then(Mono.from(multiResults1.nextUpdate()))// 6.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .block();

        //below immediately serially subscribe
        final MultiResult multiResults2;
        multiResults2 = ComQueryTask.multiStmtAsMulti(Stmts.multi(stmtGroup), adjutant);

        final AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

        Mono.from(multiResults2.nextUpdate())
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertUpdateSuccess)
                .subscribe();// 1. immediately subscribe [1] update

        Mono.from(multiResults2.nextUpdate())
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertUpdateSuccess)
                .subscribe();//2. immediately subscribe [2] update


        Flux.from(multiResults2.nextQuery())
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                    statesHolder.set(null);
                })
                .subscribe();//3. immediately subscribe [3] query

        Mono.from(multiResults2.nextUpdate())
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertUpdateSuccess)
                .subscribe();//4. immediately subscribe [4] update

        Flux.from(multiResults2.nextQuery())
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                    statesHolder.set(null);
                })
                .subscribe();//5. immediately subscribe [5] query

        Mono.from(multiResults2.nextUpdate())
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)
                .block();//6. immediately subscribe [6] update

        final Throwable error = errorHolder.get();
        if (error != null) {
            throw error;
        }
        LOG.info("multiStatement test success");
        releaseMultiStmtConnection(adjutant);
    }

    /**
     * @see ComQueryTask#batchAsMulti(StaticBatchStmt, TaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void batchAsMultiForSqlError() {
        LOG.info("multiStmtError test start");
        final TaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();
        String sql;
        final List<String> sqlGroup = new ArrayList<>(6);

        sql = "UPDATE mysql_types as t SET t.name = 'mysql' WHERE t.id = 268";//[1] update
        sqlGroup.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_date = '%s' WHERE t.id = 269", LocalDate.now());//[2] update
        sqlGroup.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 270 ORDER BY t.id LIMIT %s", ROW_COUNT);// [3] query
        sqlGroup.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = 270", LocalTime.now());// [4] update
        sqlGroup.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 270 ORDER BY t.id LIMIT %s", ROW_COUNT); //[5] query
        sqlGroup.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.not_exits_column= '%s' WHERE t.id = 271", LocalTime.now());//[6] error update sql
        sqlGroup.add(sql);

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        try {
            //below defer serially subscribe
            final MultiResult multiResults1 = ComQueryTask.batchAsMulti(Stmts.batch(sqlGroup), adjutant);
            Mono.from(multiResults1.nextUpdate())//1. immediately subscribe update
                    .switchIfEmpty(emptyError())
                    .map(this::assertUpdateSuccess)

                    .then(Mono.from(multiResults1.nextUpdate()))// 2.defer subscribe  update
                    .switchIfEmpty(emptyError())
                    .map(this::assertUpdateSuccess)

                    .thenMany(multiResults1.nextQuery())// 3.defer subscribe  query
                    .switchIfEmpty(emptyError())
                    .map(this::assertResultRow)
                    .count()
                    .flatMap(this::assertQueryRowCount)
                    .doOnNext(count -> {
                        assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                        statesHolder.set(null);
                    })

                    .then(Mono.from(multiResults1.nextUpdate()))// 4.defer subscribe  update
                    .switchIfEmpty(emptyError())
                    .map(this::assertUpdateSuccess)

                    .thenMany(multiResults1.nextQuery())// 5.defer subscribe  query
                    .switchIfEmpty(emptyError())
                    .map(this::assertResultRow)
                    .count()
                    .flatMap(this::assertQueryRowCount)
                    .doOnNext(count -> {
                        assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                        statesHolder.set(null);
                    })

                    .then(Mono.from(multiResults1.nextUpdate()))// 6.defer subscribe  update
                    .map(states -> {
                        fail("multiStmtError");
                        return states;
                    })
                    .block();

            fail("multiStmtError test failure.");
        } catch (JdbdSQLException e) {
            LOG.info("multiStmtError test success");
        } catch (Throwable e) {
            fail("multiStmtError test failure.", e);
        } finally {
            releaseMultiStmtConnection(adjutant);
        }

    }

    /**
     * @see ComQueryTask#batchAsMulti(StaticBatchStmt, TaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void batchAsMultiErrorSubscribe() {
        LOG.info("multiStmtErrorSubscribe test start");
        final TaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();

        String sql;
        final List<String> sqlGroup = new ArrayList<>(6);

        sql = "UPDATE mysql_types as t SET t.name = 'mysql' WHERE t.id = 268";//[1] update
        sqlGroup.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_date = '%s' WHERE t.id = 269", LocalDate.now());//[2] update
        sqlGroup.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 270 ORDER BY t.id LIMIT %s", ROW_COUNT);// [3] query
        sqlGroup.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = 270", LocalTime.now());// [4] update
        sqlGroup.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 270 ORDER BY t.id LIMIT %s", ROW_COUNT); //[5] query
        sqlGroup.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = 271", LocalTime.now());//[6] update
        sqlGroup.add(sql);

        try {
            final MultiResult multiResults = ComQueryTask.batchAsMulti(Stmts.batch(sqlGroup), adjutant);
            Mono.from(multiResults.nextUpdate())//[1] update
                    .map(this::assertUpdateSuccess)

                    .then(Mono.from(multiResults.nextUpdate())) //[2] update
                    .map(this::assertUpdateSuccess)

                    .then(Mono.from(multiResults.nextUpdate())) //[3] query ,error subscribe
                    .map(this::assertUpdateSuccess)
                    .block();
            fail("multiStmtErrorSubscribe test failure");
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.UPDATE, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.QUERY, "getActualType");
        } catch (Throwable e) {
            fail("multiStmtErrorSubscribe test failure", e);
        }

        try {
            final MultiResult multiResults = ComQueryTask.batchAsMulti(Stmts.batch(sqlGroup), adjutant);
            Mono.from(multiResults.nextUpdate())//[1] update
                    .map(this::assertUpdateSuccess)

                    .thenMany(multiResults.nextQuery()) //[2] update,error subscribe
                    .map(this::assertResultRow)

                    .then()
                    .block();
            fail("multiStmtErrorSubscribe test failure");
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.QUERY, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.UPDATE, "getActualType");
        } catch (Throwable e) {
            fail("multiStmtErrorSubscribe test failure", e);
        }

        LOG.info("multiStmtErrorSubscribe test success");
        releaseMultiStmtConnection(adjutant);
    }

    /**
     * @see ComQueryTask#batchAsMulti(StaticBatchStmt, TaskAdjutant)
     */
    @Test(timeOut = TIME_OUT, invocationCount = 3)
    public void multiTooManySubscribe() {
        LOG.info("multiStmtTooManySubscribe test start");
        final TaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();
        String sql;
        final List<String> sqlGroup = new ArrayList<>(3);

        sql = "UPDATE mysql_types as t SET t.name = 'mysql' WHERE t.id = 268";//[1] update
        sqlGroup.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_date = '%s' WHERE t.id = 269", LocalDate.now());//[2] update
        sqlGroup.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 270 ORDER BY t.id LIMIT %s", ROW_COUNT);// [3] query
        sqlGroup.add(sql);

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        try {
            //below defer serially subscribe
            final MultiResult multiResults1 = ComQueryTask.batchAsMulti(Stmts.batch(sqlGroup), adjutant);
            Mono.from(multiResults1.nextUpdate())//1. defer subscribe update
                    .switchIfEmpty(emptyError())
                    .map(this::assertUpdateSuccess)
                    .doOnNext(r -> LOG.debug("multiStmtTooManySubscribe 1. immediately subscribe update"))

                    .then(Mono.from(multiResults1.nextUpdate()))// 2.defer subscribe  update
                    .switchIfEmpty(emptyError())
                    .map(this::assertUpdateSuccess)
                    .doOnNext(r -> LOG.debug("multiStmtTooManySubscribe  2.defer subscribe  update"))

                    .thenMany(multiResults1.nextQuery(statesHolder::set))// 3.defer subscribe  query
                    .switchIfEmpty(emptyError())
                    .map(this::assertResultRow)
                    .count()
                    .doOnNext(count -> {
                        assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                        statesHolder.set(null);
                    })
                    .doOnNext(r -> LOG.debug("multiStmtTooManySubscribe   3.defer subscribe  query"))
                    .flatMap(this::assertQueryRowCount)


                    .then(Mono.from(multiResults1.nextUpdate()))// 4. error no more result
                    .map(states -> {
                        fail("multiStmtTooManySubscribe");
                        return states;
                    })

                    .block();

            fail("multiStmtTooManySubscribe test failure.");
        } catch (NoMoreResultException e) {
            LOG.info("multiStmtTooManySubscribe test success");
        } catch (Throwable e) {
            fail("multiStmtTooManySubscribe test failure.", e);
        } finally {
            releaseMultiStmtConnection(adjutant);
        }

        releaseMultiStmtConnection(adjutant);
    }

    /**
     * @see ComQueryTask#bindBatch(BindBatchStmt, TaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void bindableBatchWithMultiStmtMode() {
        LOG.info("bindableBatchWithMultiStmtMode test start");
        final TaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();
        assertTrue(Capabilities.supportMultiStatement(adjutant.capability()), "negotiatedCapability");

        final String sql = "UPDATE mysql_types as t SET t.my_long_text = ? WHERE t.id = ?";
        final List<List<BindValue>> groupList = new ArrayList<>(4);
        List<BindValue> paramGroup;

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.wrap(0, MySQLType.LONGTEXT, "bindable batch update 1"));
        paramGroup.add(BindValue.wrap(1, MySQLType.BIGINT, 271));
        groupList.add(paramGroup);

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.wrap(0, MySQLType.LONGTEXT, "bindable batch update 2"));
        paramGroup.add(BindValue.wrap(1, MySQLType.BIGINT, 272));
        groupList.add(paramGroup);

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.wrap(0, MySQLType.LONGTEXT, "bindable batch update 3"));
        paramGroup.add(BindValue.wrap(1, MySQLType.BIGINT, 273));
        groupList.add(paramGroup);

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.wrap(0, MySQLType.LONGTEXT, "bindable batch update 4"));
        paramGroup.add(BindValue.wrap(1, MySQLType.BIGINT, 274));
        groupList.add(paramGroup);

        final List<ResultStates> resultStatesList;
        resultStatesList = ComQueryTask.paramBatchUpdate(Stmts.batchBind(sql, groupList), adjutant)
                .collectList()
                .block();

        assertNotNull(resultStatesList, "resultStatesList");
        assertEquals(resultStatesList.size(), groupList.size(), "resultStatesList");

        for (ResultStates states : resultStatesList) {
            assertEquals(states.affectedRows(), 1L, "getAffectedRows");
        }

        LOG.info("bindableBatchWithMultiStmtMode test success");
        releaseConnection(adjutant);
    }

    /**
     * @see ComQueryTask#batchUpdate(StaticBatchStmt, TaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void batchUpdateWithMultiStmtMode() {
        LOG.info("batchUpdateWithMultiStmtMode test start");
        final TaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();
        assertTrue(Capabilities.supportMultiStatement(adjutant.capability()), "negotiatedCapability");

        String sql;
        final List<String> sqlList = new ArrayList<>(5);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0' WHERE t.id = 275";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 276";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 2' WHERE t.id = 277";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 3' WHERE t.id = 278";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 4' WHERE t.id = 279";
        sqlList.add(sql);

        List<ResultStates> resultStatesList;

        resultStatesList = ComQueryTask.batchUpdate(Stmts.batch(sqlList), adjutant)
                .collectList()
                .block();

        assertNotNull(resultStatesList, "resultStatesList");
        assertEquals(resultStatesList.size(), sqlList.size(), "resultStatesList");

        for (ResultStates states : resultStatesList) {
            assertEquals(states.affectedRows(), 1L, "getAffectedRows");
        }

        LOG.info("batchUpdateWithMultiStmtMode test success");
        releaseConnection(adjutant);
    }


    /*################################## blow private method ##################################*/

    private Mono<Void> assertQueryRowCount(Long resultRowCount) {
        return resultRowCount == ROW_COUNT
                ? Mono.empty()
                : Mono.error(new RuntimeException(String.format("Query result row count[%s] error", resultRowCount)));
    }


    private ResultStates assertUpdateSuccess(ResultStates states) {
        Assert.assertEquals(states.affectedRows(), 1L, "update rows");
        return states;
    }


    private ResultRow assertResultRow(ResultRow row) {
        LOG.trace("id:{},name:{},createTime:{}",
                row.getNonNull("id", Long.class),
                row.getNonNull("name", String.class),
                row.getNonNull("createTime", LocalDateTime.class)
        );
        return row;
    }

    private <T> Mono<T> emptyError() {
        return Mono.defer(() -> Mono.error(new RuntimeException("Query result set is empty.")));
    }


    protected static TaskAdjutant obtainMultiStmtTaskAdjutant() {
        throw new UnsupportedOperationException();
    }

    protected static void releaseMultiStmtConnection(TaskAdjutant adjutant) {
        MULTI_STMT_TASK_ADJUTANT_QUEUE.add(adjutant);
    }


    private static SessionAdjutant createMultiStmtSessionAdjutant() {
        Map<String, String> map = new HashMap<>();
        if (ClientTestUtils.existsServerPublicKey()) {
            map.put(MyKey.sslMode.getKey(), Enums.SslMode.DISABLED.name());
        }
        ClientTestUtils.appendZoneConfig(map);
        map.put(MyKey.allowMultiQueries.getKey(), "true");
        return createSessionAdjutantForSingleHost(map);
    }


}
