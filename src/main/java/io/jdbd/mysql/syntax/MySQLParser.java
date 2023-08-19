package io.jdbd.mysql.syntax;

import io.jdbd.JdbdException;
import io.jdbd.vendor.syntax.SQLParser;

public interface MySQLParser extends SQLParser {

    @Override
    MySQLStatement parse(String singleSql) throws JdbdException;

    boolean isSingleStmt(String sql) throws JdbdException;

    boolean isMultiStmt(String sql) throws JdbdException;

}
