package io.jdbd.mysql.util;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.session.Isolation;
import io.jdbd.session.TransactionOption;

public abstract class MySQLProtocolUtil {

    private MySQLProtocolUtil() {
        throw new UnsupportedOperationException();
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-transaction.html">SET TRANSACTION Statement</a>
     */
    @Nullable
    public static JdbdException setTransactionOption(final TransactionOption option, final StringBuilder builder) {
        final Isolation isolation = option.isolation();

        if (isolation != null) {
            builder.append("SET TRANSACTION ISOLATION LEVEL ");
            if (appendIsolation(isolation, builder)) {
                return MySQLExceptions.unknownIsolation(isolation);
            }
            builder.append(Constants.SPACE_COMMA_SPACE);
        }

        if (option.isReadOnly()) {
            builder.append("READ ONLY");
        } else {
            builder.append("READ WRITE");
        }
        return null;
    }


    public static boolean appendIsolation(final Isolation isolation, final StringBuilder builder) {

        boolean error = false;
        if (isolation == Isolation.READ_COMMITTED) {
            builder.append("READ COMMITTED");
        } else if (isolation == Isolation.REPEATABLE_READ) {
            builder.append("REPEATABLE READ");
        } else if (isolation == Isolation.SERIALIZABLE) {
            builder.append("SERIALIZABLE");
        } else if (isolation == Isolation.READ_UNCOMMITTED) {
            builder.append("READ UNCOMMITTED");
        } else {
            error = true;
        }
        return error;
    }


}
