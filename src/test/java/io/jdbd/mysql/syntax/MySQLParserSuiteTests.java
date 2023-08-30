package io.jdbd.mysql.syntax;


import io.jdbd.JdbdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class MySQLParserSuiteTests {

    static final Logger LOG = LoggerFactory.getLogger(MySQLParserSuiteTests.class);


    @Test
    public void onlyBlockComment() throws Exception {
        final String sql = "/* ping */";
        MySQLParser parser = DefaultMySQLParser.getForInitialization();
        assertTrue(parser.isSingleStmt(sql));
    }

    @Test(expectedExceptions = JdbdException.class)
    public void blockCommentNotClose() {
        final String sql = "/* ping \n\r \n";
        MySQLParser parser = DefaultMySQLParser.getForInitialization();
        parser.isSingleStmt(sql);
    }

    @Test
    public void singleStatement() {
        LOG.info("singleStatement test start");
        MySQLParser parser = DefaultMySQLParser.getForInitialization();
        final String updateSql = "/* this is a single statement. */ update user as u set u.name = 'zoro' where u.id = 2";
        assertTrue(parser.isSingleStmt(updateSql));

        final String deleteSql = "/* this is a single statement. */ delete user where user.id = 3 /* this is a single statement. */ -- this is delete sql.";
        assertTrue(parser.isSingleStmt(deleteSql));

        final String insertSql = "/* this is a single statement. */ insert into user(id,name) value(1,'zoro')";
        assertTrue(parser.isSingleStmt(insertSql));


        final String selectSql = "/* this is a single statement. */ select u.* from user as u ## this is a select statement.";
        assertTrue(parser.isSingleStmt(selectSql));

        LOG.info("singleStatement test success");
    }

    @Test
    public void notSingleStatement() throws SQLException {
        LOG.info("notSingleStatement test start");
        final String sql = "/* this isn't a single statement. */ update user as u set u.name = 'zoro' where u.id = 2; select u.* from user as u";
        MySQLParser parser = DefaultMySQLParser.getForInitialization();
        assertFalse(parser.isSingleStmt(sql));

        LOG.info("notSingleStatement test success");
    }

    @Test
    public void multiStatement() throws SQLException {
        LOG.info("multiStatement test start");
        final String sql = "/* this is a multi statement. */ update user as u set u.name = 'zoro' where u.id = 2; select u.* from user as u";
        MySQLParser parser = DefaultMySQLParser.getForInitialization();
        assertTrue(parser.isMultiStmt(sql));

        LOG.info("multiStatement test success");

    }

    @Test(expectedExceptions = JdbdException.class)
    public void quoteNotClose() {
        LOG.info("quoteNotClose test start");
        final String sql = "/* this is a multi statement. */ update user as u set u.name = 'zoro where u.id = 2";
        MySQLParser parser = DefaultMySQLParser.getForInitialization();
        assertTrue(parser.isSingleStmt(sql));

        LOG.info("quoteNotClose test success");
    }

    @Test
    public void quoteEscaped() throws SQLException {
        LOG.info("quoteEscaped test start");
        final String sql = "/* this is a multi statement. */ update user as u set u.name = 'zoro\\'simple\\'captain' where u.id = 2";
        LOG.info("quoteEscaped sql :{}", sql);
        MySQLParser parser = DefaultMySQLParser.getForInitialization();
        assertTrue(parser.isSingleStmt(sql));

        LOG.info("quoteEscaped test success");
    }


}
