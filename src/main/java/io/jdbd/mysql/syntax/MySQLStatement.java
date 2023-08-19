package io.jdbd.mysql.syntax;

import io.jdbd.vendor.syntax.SQLStatement;

public interface MySQLStatement extends SQLStatement {


    boolean isLoadInfile();

}
