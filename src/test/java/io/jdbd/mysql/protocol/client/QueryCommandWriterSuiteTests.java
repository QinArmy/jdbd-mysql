package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Groups;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.stmt.ParamStmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @see QueryCommandWriter
 */
@Test(groups = {Groups.COM_QUERY_WRITER}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS})
public class QueryCommandWriterSuiteTests extends AbstractStmtTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(QueryCommandWriterSuiteTests.class);

    public QueryCommandWriterSuiteTests() {
        super(SubType.COM_QUERY);
    }

    @Override
    Mono<ResultStates> executeUpdate(ParamStmt stmt, TaskAdjutant adjutant) {
        return ComQueryTask.paramUpdate(stmt, adjutant);
    }

    @Override
    Flux<ResultRow> executeQuery(ParamStmt stmt, TaskAdjutant adjutant) {
        return ComQueryTask.paramQuery(stmt, DatabaseProtocol.ROW_FUNC, adjutant);
    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }


    @Test(timeOut = TIME_OUT)
    public void createStaticSingleCommand() {
        LOG.info("createStaticSingleCommand test start");
        //final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        //TODO zoro add test code
        LOG.info("createStaticSingleCommand test success");
        // releaseConnection(adjutant);
    }


}
