package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.stmt.ParamStmt;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;

public class ComQueryDataTypeSuiteTests extends AbstractDataTypeSuiteTests {

    public ComQueryDataTypeSuiteTests() {
        super(100);
    }

    @Override
    Mono<ResultStates> executeUpdate(ParamStmt stmt, TaskAdjutant adjutant) {
        return ComQueryTask.paramUpdate(stmt, adjutant);
    }

    @Override
    Flux<ResultRow> executeQuery(ParamStmt stmt, TaskAdjutant adjutant) {
        return ComQueryTask.paramQuery(stmt, DatabaseProtocol.ROW_FUNC, adjutant);
    }

    /**
     * @see MySQLType#TINYINT
     * @see MySQLType#TINYINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void tinyIntBindAndGet() {
        tinyInt();
    }

    /**
     * @see MySQLType#SMALLINT
     * @see MySQLType#SMALLINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void smallIntBindAndGet() {
        smallInt();
    }

    /**
     * @see MySQLType#MEDIUMINT
     * @see MySQLType#MEDIUMINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void mediumIntBindAndGet() {
        mediumInt();
    }

    /**
     * @see MySQLType#INT
     * @see MySQLType#INT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void intBindAndGet() {
        integer();
    }

    /**
     * @see MySQLType#BIGINT
     * @see MySQLType#BIGINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void bigIntBindAndGet() {
        bigInt();
    }

    /**
     * @see MySQLType#DECIMAL
     * @see MySQLType#DECIMAL_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void decimalBindAndGet() {
        decimal();
    }

    /**
     * @see MySQLType#FLOAT
     * @see MySQLType#FLOAT_UNSIGNED
     */
    @SuppressWarnings("deprecation")
    @Test(timeOut = TIME_OUT)
    public void floatBindAndGet() {
        floatType();
    }

    /**
     * @see MySQLType#DOUBLE
     * @see MySQLType#DOUBLE_UNSIGNED
     */
    @SuppressWarnings("deprecation")
    @Test(timeOut = TIME_OUT)
    public void doubleBindAndGet() {
        doubleType();
    }

    /**
     * @see MySQLType#BIT
     */
    @Test
    public void bitBindAndGet() {
        bitType();
    }

    /**
     * @see MySQLType#CHAR
     */
    @Test(timeOut = TIME_OUT)
    public void charBindAndGet() {
        charType();
    }

    /**
     * @see MySQLType#VARCHAR
     */
    @Test(timeOut = TIME_OUT)
    public void varCharBindAndGet() {
        varChar();
    }

    /**
     * @see MySQLType#BINARY
     */
    @Test(timeOut = TIME_OUT)
    public void binaryBindAndGet() {
        binary();
    }

    /**
     * @see MySQLType#VARBINARY
     */
    @Test(timeOut = TIME_OUT)
    public void varBinaryBindAndGet() {
        varBinary();
    }

    /**
     * @see MySQLType#VARBINARY
     */
    @Test
    public void enumBindAndGet() {
        enumType();
    }


    /**
     * @see MySQLType#SET
     */
    @Test
    public void setBindAndGet() {
        setType();
    }

    /**
     * @see MySQLType#TIME
     */
    @Test
    public void timeBindAndGet() {
        time();
    }

    /**
     * @see MySQLType#DATE
     */
    @Test
    public void dateBindAndGet() {
        date();
    }

    /**
     * @see MySQLType#YEAR
     */
    @Test
    public void yearBindAndGet() {
        year();
    }

    /**
     * @see MySQLType#TIMESTAMP
     */
    @Test
    public void timestampBindAndGet() {
        timestamp();
    }

    /**
     * @see MySQLType#DATETIME
     */
    @Test
    public void datetimeBindAndGet() {
        dateTime();
    }

    /**
     * @see MySQLType#TINYTEXT
     */
    @Test
    public void tinyTextBindAndGet() {
        tinyText();
    }

    /**
     * @see MySQLType#TEXT
     */
    @Test
    public void textBindAndGet() {
        text();
    }

    /**
     * @see MySQLType#MEDIUMTEXT
     */
    @Test
    public void mediumTextBindAndGet() {
        mediumText();
    }

    /**
     * @see MySQLType#LONGTEXT
     */
    @Test
    public void longTextBindAndGet() throws IOException {
        longText();
    }

    /**
     * @see MySQLType#TINYBLOB
     */
    @Test
    public void tinyBlobBindAndGet() {
        tinyBlob();
    }

    /**
     * @see MySQLType#BLOB
     */
    @Test
    public void blobBindAndGet() {
        blob();
    }

    /**
     * @see MySQLType#MEDIUMBLOB
     */
    @Test
    public void mediumBlobBindAndGet() {
        mediumBlob();
    }

    /**
     * @see MySQLType#LONGBLOB
     */
    @Test
    public void longBlobBindAndGet() throws IOException {
        longBlob();
    }


}
