package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.Groups;
import io.jdbd.mysql.TestKey;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.Stmts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;

import static org.testng.Assert.*;

/**
 * <p>
 * This class test LOAD DATA LOCAL statement.
 * </p>
 *
 * @see ComQueryTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_local_infile_request.html">Protocol::LOCAL INFILE Request</a>
 */
@Test(groups = {Groups.LOAD_DATA}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS
        , Groups.COM_QUERY_WRITER})
public class LoadDataLocalSuiteTests extends AbstractStmtTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(LoadDataLocalSuiteTests.class);

    public LoadDataLocalSuiteTests() {
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

    @BeforeClass(timeOut = TIME_OUT)
    public static void beforeClass() {
        LOG.info("{} beforeClass test start", Groups.LOAD_DATA);

        final TaskAdjutant adjutant = obtainTaskAdjutant();

        final String ddl = "CREATE TABLE IF NOT EXISTS mysql_load_data(\n" +
                "    id bigint AUTO_INCREMENT,\n" +
                "    create_time datetime NOT NULL  DEFAULT current_timestamp,\n" +
                "    name VARCHAR(30) NOT  NULL DEFAULT '',\n" +
                "    PRIMARY KEY (id)\n" +
                ")";

        LOG.info("{} ddl:\n:{}", Groups.LOAD_DATA, ddl);

        ComQueryTask.update(Stmts.stmt(ddl), adjutant)
                .doOnNext(Assert::assertNotNull)
                .then(ComQueryTask.update(Stmts.stmt("TRUNCATE mysql_load_data"), adjutant))
                .doOnNext(Assert::assertNotNull)
                .then()
                .block();

        releaseConnection(adjutant);
        LOG.info("{} beforeClass test success", Groups.LOAD_DATA);
    }

    @AfterClass(timeOut = TIME_OUT)
    public static void afterClass() {
        LOG.info("{} afterClass test start", Groups.LOAD_DATA);

        if (ClientTestUtils.getTestConfig().isOn(TestKey.TRUNCATE_AFTER_SUITE)) {

            final TaskAdjutant adjutant = obtainTaskAdjutant();
            final String sql = "TRUNCATE mysql_load_data";

            ResultStates status;
            status = ComQueryTask.update(Stmts.stmt(sql), adjutant)
                    .block();

            assertNotNull(status, sql);

            releaseConnection(adjutant);
        }

        LOG.info("{} afterClass test success", Groups.LOAD_DATA);

    }


    @Test(timeOut = TIME_OUT)
    public void loadDataLocal() throws IOException {
        LOG.info("loadDataLocal test start");

        final int rowCount = 5000;
        final Path path;
        path = createDataFile("mysql_load_data", rowCount);
        try {
            doLoadData(path, rowCount);
        } finally {
            Files.deleteIfExists(path);
        }
        LOG.info("loadDataLocal test success");
    }

    @Test(timeOut = 10L * 1000L)
    public void loadDataLocal22m() throws IOException {
        LOG.info("loadDataLocal22m test start");

        final int rowCount = 600000;
        final Path path;
        path = createDataFile("mysql_load_data_22m", rowCount);

        try {
            doLoadData(path, rowCount);
        } finally {
            Files.deleteIfExists(path);
        }
        LOG.info("loadDataLocal22m test success");
    }




    /*################################## blow private method ##################################*/

    private Path createDataFile(String prefix, int rowCount) throws IOException {
        final Path path = Files.createTempFile(prefix, ".csv");

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            final String now = LocalDateTime.now().format(MySQLTimes.MYSQL_DATETIME_FORMATTER_0);
            StringBuilder builder = new StringBuilder(rowCount * 25);
            for (int i = 0; i < rowCount; i++) {
                if (i > 0) {
                    builder.append("\n");
                }
                builder.append(now)
                        .append("\t")
                        .append("army")
                        .append(i);
            }
            byte[] bytes = builder.toString().getBytes(StandardCharsets.UTF_8);
            channel.write(ByteBuffer.wrap(bytes));
            return path;
        } catch (Throwable e) {
            Files.deleteIfExists(path);
            throw e;
        }
    }

    private void doLoadData(Path path, long rows) {
        final TaskAdjutant adjutant = obtainTaskAdjutant();
        if (!adjutant.host().properties().getOrDefault(MySQLKey.ALLOW_LOAD_LOCAL_INFILE)) {
            fail(String.format("client no support Load data local statement,please config property[%s]",
                    MySQLKey.ALLOW_LOAD_LOCAL_INFILE));
        }
        if (!adjutant.sessionEnv().isSupportLocalInfile()) {
            LOG.warn("Server no support Local infile ,please config system variables[@@GLOBAL.local_infile]");
            return;
        }


        final String sql = String.format("LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE mysql_load_data (create_time,name)"
                , path.toAbsolutePath());

        LOG.info("execute loadDataLocal sql :{} ", sql);
        ResultStates status;
        status = ComQueryTask.update(Stmts.stmt(sql), adjutant)
                .block();

        assertNotNull(status, sql);
        LOG.info("Local file:{} affectedRows:{}", path, status.affectedRows());
        assertEquals(status.affectedRows(), rows, sql);

        releaseConnection(adjutant);
    }


}
