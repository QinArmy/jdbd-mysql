package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.Groups;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.stmt.JdbdParamValue;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * <p>
 * id range [51,99]
 * </p>
 *
 * @see ComPreparedTask
 */
@Test(groups = {Groups.COM_STMT_PREPARE}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS, Groups.DATA_PREPARE})
public class ComPreparedTaskSuiteTests extends AbstractStmtTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComPreparedTaskSuiteTests.class);

    public ComPreparedTaskSuiteTests() {
        super(SubType.COM_PREPARE_STMT);
    }

    @Override
    Mono<ResultStates> executeUpdate(BindStmt stmt, TaskAdjutant adjutant) {
        return ComPreparedTask.update(stmt, adjutant);
    }

    @Override
    Flux<ResultRow> executeQuery(BindStmt stmt, TaskAdjutant adjutant) {
        return ComPreparedTask.query(stmt, adjutant);
    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }

    /**
     * @see ComPreparedTask#update(ParamStmt, TaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void update() {
        LOG.info("prepare update test start");
        final TaskAdjutant adjutant = obtainTaskAdjutant();
        String sql;
        List<ParamValue> bindValueList;
        ResultStates states;

        sql = "UPDATE mysql_types as t SET t.my_tiny_text = ? WHERE t.id = ?";
        bindValueList = new ArrayList<>(2);
        bindValueList.add(JdbdParamValue.wrap(0, "prepare update 1"));
        bindValueList.add(JdbdParamValue.wrap(1, 80L));

        states = ComPreparedTask.update(Stmts.multiPrepare(sql, bindValueList), adjutant)
                .block();

        assertNotNull(states, "states");
        assertEquals(states.affectedRows(), 1L, "getAffectedRows");

        LOG.info("prepare update test success");
        releaseConnection(adjutant);
    }


    @Test(timeOut = TIME_OUT)
    public void bigIntBindAndExtract() {
        doBigIntBindAndExtract(LOG);
    }


    @Test(timeOut = TIME_OUT)
    public void dateBindAndExtract() {
        doDateBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void timeBindAndExtract() {
        doTimeBindAndExtract(LOG);
    }


    @Test(timeOut = TIME_OUT)
    public void datetimeBindAndExtract() {
        doDatetimeBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void stringBindAndExtract() {
        doStringBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void binaryBindAndExtract() {
        doBinaryBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void doBitBindAndExtract() {
        try {
            doBitBindAndExtract(LOG);
        } catch (JdbdSQLException e) {
            LOG.error("doBitBindAndExtract code:{},states:{}", e.getVendorCode(), e.getSQLState());
            throw e;
        }
    }

    @Test(timeOut = TIME_OUT)
    public void tinyint1BindExtract() {
        doTinyint1BindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void numberBindAndExtract() {
        doNumberBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void enumBindAndExtract() {
        doEnumBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void setTypeBindAndExtract() {
        doSetTypeBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void jsonBindAndExtract() throws Exception {
        doJsonBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void tinyBlobBindAndExtract() {
        doTinyBlobBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void blobBindAndExtract() {
        doBlobBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void mediumBlobBindAndExtract() {
        doMediumBlobBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void longBlobBindAndExtract() {
        doLongBlobBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void tinyTextBindAndExtract() {
        doTinyTextBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void textBindAndExtract() {
        doTextBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void mediumTextBindAndExtract() {
        doMediumTextBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void longTextBindAndExtract() {
        doLongTextBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void geometryBindAndExtract() {
        doGeometryBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void pointBindAndExtract() {
        doPointBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void lineStringBindAndExtract() {
        doLineStringBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void polygonBindAndExtract() {
        doPolygonBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void multiPointBindExtract() {
        doMultiPointBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void multiLineStringBindExtract() {
        doMultiLineStringBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void multiPolygonBindExtract() {
        doMultiPolygonBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void geometryCollectionBindExtract() {
        doGeometryCollectionBindExtract(LOG);
    }


}
