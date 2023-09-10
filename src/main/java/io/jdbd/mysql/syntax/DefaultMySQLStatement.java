package io.jdbd.mysql.syntax;

import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.util.MySQLCollections;

import java.util.List;
import java.util.function.Predicate;

final class DefaultMySQLStatement implements MySQLStatement {

    private final String sql;

    private final List<String> staticSqlList;

    private final boolean backslashEscapes;

    private final boolean ansiQuotes;

    DefaultMySQLStatement(String sql, List<String> staticSqlList, boolean backslashEscapes, boolean ansiQuotes) {
        this.sql = sql;
        this.staticSqlList = MySQLCollections.unmodifiableList(staticSqlList);
        this.backslashEscapes = backslashEscapes;
        this.ansiQuotes = ansiQuotes;
    }

    @Override
    public List<String> sqlPartList() {
        return this.staticSqlList;
    }

    @Override
    public String originalSql() {
        return this.sql;
    }

    @Override
    public boolean isInvalid(Predicate<SQLMode> func) {
        return func.test(SQLMode.ANSI_QUOTES) != this.ansiQuotes
                || func.test(SQLMode.NO_BACKSLASH_ESCAPES) == this.backslashEscapes;
    }

}
