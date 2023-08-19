package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLNumbers;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * This class test query big column ,eg: {@link io.jdbd.mysql.MySQLType#LONGBLOB}
 *
 * @see ComQueryTask
 * @see TextResultSetReader
 */
@Test
//(groups = {Groups.TEXT_RESULT_BIG_COLUMN}, dependsOnGroups = {Groups.COM_QUERY, Groups.DATA_PREPARE, Groups.COM_STMT_PREPARE})
public class ComQueryTaskBigColumnSuiteTests extends AbstractTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTaskBigColumnSuiteTests.class);

    @Test(enabled = false)
    public void longBlob() throws Exception {
        LOG.info("longBlob test start");
        final Path longBlobFile;
        longBlobFile = crateLongBlobFile();

        Files.deleteIfExists(longBlobFile);
        LOG.info("longBlob test end");
    }

    @Test(enabled = false, timeOut = TIME_OUT)
    public void myBit20() {
        final TaskAdjutant adjutant = obtainTaskAdjutant();
        String sql, alias;
        List<ParamValue> list;

        alias = "call";

        sql = "CALL queryNow(?,?)";
        list = new ArrayList<>(2);
        list.add(JdbdValues.paramValue(0, MySQLType.INT, 0));
        list.add(JdbdValues.paramValue(1, MySQLType.VARCHAR, ""));
        ResultStates states;
        states = ComPreparedTask.update(Stmts.paramStmt(sql, list), adjutant)
                .block();
        assertNotNull(states, alias);
        // assertEquals(states.getAffectedRows(), 1L, "myBit20");

//        alias = "call";
//        sql = "CALL queryNow(?,?)";
//        list = new ArrayList<>(2);
//        list.add(MySQLParamValue.create(0, 0));
//        list.add(MySQLParamValue.create(0, ""));
//      List<ResultRow> rowList;
//        rowList = ComPreparedTask.query(StmtWrappers.multiPrepare(sql, list), adjutant)
//                .collectList()
//                .block();
//        assertNotNull(rowList, alias);
//        assertFalse(rowList.isEmpty(),alias);
//        LOG.info("{}:{}", alias, rowList.get(0).getNonNull(alias));
        releaseConnection(adjutant);
    }


    private Path crateLongBlobFile() throws Exception {
        final Random random = new Random();
        final byte[] blockArray = new byte[1024];
        for (int i = 0, offset = 0; i < 128; i++) {
            MySQLNumbers.longToBigEndian(random.nextLong(), blockArray, offset, 8);
            offset += 8;
        }
        final Path dir = ClientTestUtils.getBigColumnTestPath();
        if (Files.notExists(dir)) {
            Files.createDirectories(dir);
        }
        final Path longBlobFile = Files.createTempFile(dir, "longBlob", ".b");

        try (FileChannel channel = FileChannel.open(longBlobFile, StandardOpenOption.WRITE)) {
            final ByteBuffer buffer = ByteBuffer.wrap(blockArray);
            buffer.position(buffer.limit());
            final int end = 1 << 22;
            for (int i = 1; i <= end; i++) {
                buffer.flip();
                if (i == end) {
                    buffer.get();
                }
                channel.write(buffer);
                buffer.clear();
                buffer.position(buffer.limit());
            }
            assertEquals(channel.size(), (1L << 32) - 1L, "size");
            return longBlobFile;
        } catch (Throwable e) {
            Files.deleteIfExists(longBlobFile);
            throw e;
        }


    }


}
