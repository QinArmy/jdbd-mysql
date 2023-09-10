package io.jdbd.mysql.syntax;

import io.jdbd.mysql.SQLMode;
import io.jdbd.vendor.syntax.SQLStatement;

import java.util.function.Predicate;

public interface MySQLStatement extends SQLStatement {

    boolean isInvalid(Predicate<SQLMode> func);


}
