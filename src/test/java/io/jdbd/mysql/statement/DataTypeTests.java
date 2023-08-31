package io.jdbd.mysql.statement;

import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.statement.BindSingleStatement;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.function.Function;

/**
 * <p>
 * This class is responsible for test data type bind and get.
 * </p>
 * <p>
 * All test method's session parameter is closed by {@link #closeSessionAfterTest(Method, ITestContext)}
 * </p>
 */
public class DataTypeTests extends SessionTestSupport {

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
     * @see JdbdType#MEDIUMINT
     * @see JdbdType#MEDIUMINT_UNSIGNED
     * @see MySQLType#MEDIUMINT
     * @see MySQLType#MEDIUMINT_UNSIGNED
     * @see <a href="https://dev.mysql.com/doc/refman/8.1/en/integer-types.html">Integer Types </a>
     */
    @Test(invocationCount = 3, dataProvider = "mediumIntStmtProvider")
    public void mediumInt(final BindSingleStatement insertStmt, final BindSingleStatement queryInsert) {
        insertStmt.bind(0, JdbdType.MEDIUMINT, 0)
                .bind(1, JdbdType.MEDIUMINT_UNSIGNED, 0)

                // min value
                .bind(2, JdbdType.MEDIUMINT, -0x80_00_00) // -8388608
                .bind(3, JdbdType.MEDIUMINT_UNSIGNED, 0)

                // max value
                .bind(4, JdbdType.MEDIUMINT, 0x7f_ff_ff)
                .bind(5, JdbdType.MEDIUMINT_UNSIGNED, 0xff_ff_ff)

                // other
                .bind(6, JdbdType.MEDIUMINT, 6666)
                .bind(7, JdbdType.MEDIUMINT_UNSIGNED, 8888);


        final Function<CurrentRow, ResultRow> function;
        function = row -> {
            LOG.info("row id : {}", row.get(0, Long.class));
            switch ((int) row.rowNumber()) {
                case 1: {
                    Assert.assertEquals(row.get(1, Integer.class), 0);
                    Assert.assertEquals(row.get(2, Integer.class), 0);
                }
                break;
                case 2: {
                    Assert.assertEquals(row.get(1, Integer.class), -0x80_00_00);
                    Assert.assertEquals(row.get(2, Integer.class), 0);
                }
                break;
                case 3: {
                    Assert.assertEquals(row.get(1, Integer.class), 0x7f_ff_ff);
                    Assert.assertEquals(row.get(2, Integer.class), 0xff_ff_ff);
                }
                break;
                case 4: {
                    Assert.assertEquals(row.get(1, Integer.class), 6666);
                    Assert.assertEquals(row.get(2, Integer.class), 8888);
                }
                break;
                default:
                    throw new RuntimeException("unknown row");

            }
            return row.asResultRow();
        };


        Mono.from(insertStmt.executeUpdate())
                .flatMapMany(s -> {
                    LOG.info("affectedRows : {} , lastId : {}", s.affectedRows(), s.lastInsertedId());
                    final int rowCount = (int) s.affectedRows();
                    long lastId = s.lastInsertedId();
                    for (int i = 0; i < rowCount; i++) {
                        queryInsert.bind(i, JdbdType.BIGINT, lastId);
                        lastId++;
                    }
                    return Flux.from(queryInsert.executeQuery(function));
                })
                .blockLast();


    }


    /**
     * @see JdbdType#MEDIUMINT
     * @see JdbdType#MEDIUMINT_UNSIGNED
     * @see MySQLType#MEDIUMINT
     * @see MySQLType#MEDIUMINT_UNSIGNED
     */
    @DataProvider(name = "mediumIntStmtProvider", parallel = true)
    public final Object[][] mediumIntStmtProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String insertSql, querySql;

        insertSql = "INSERT mysql_types(my_mediumint,my_mediumint_unsigned) VALUES (?,?),(?,?),(?,?),(?,?)";
        querySql = "SELECT t.id,t.my_mediumint,t.my_mediumint_unsigned FROM mysql_types AS t WHERE t.id IN (?,?,?,?) ORDER BY t.id";

        final BindSingleStatement insertStmt, queryInsert;
        insertStmt = createSingleStatement(targetMethod, context, insertSql);
        queryInsert = createSingleStatement(targetMethod, context, querySql);

        return new Object[][]{{insertStmt, queryInsert}};
    }


}
