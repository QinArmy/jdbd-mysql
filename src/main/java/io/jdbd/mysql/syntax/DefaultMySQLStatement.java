package io.jdbd.mysql.syntax;

import io.jdbd.mysql.util.MySQLCollections;

import java.util.List;

final class DefaultMySQLStatement implements MySQLStatement {

    private final String sql;

    private final List<String> staticSqlList;

    private final boolean loadInfile;

    DefaultMySQLStatement(String sql, List<String> staticSqlList, boolean loadInfile) {
        this.sql = sql;
        this.staticSqlList = MySQLCollections.unmodifiableList(staticSqlList);
        this.loadInfile = loadInfile;
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
    public boolean isLoadInfile() {
        return this.loadInfile;
    }
}
