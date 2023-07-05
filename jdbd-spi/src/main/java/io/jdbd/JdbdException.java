package io.jdbd;


import io.jdbd.lang.Nullable;


/**
 * @see JdbdSQLException
 * @see JdbdNonSQLException
 */
public class JdbdException extends RuntimeException {

    public JdbdException(String message) {
        super(message);
    }


    public JdbdException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    @Deprecated
    JdbdException(@Nullable Throwable cause, String message) {
        super(message, cause);
    }

    public JdbdException(String message, @Nullable Throwable cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }


    protected static String createMessage(@Nullable String messageFormat, @Nullable Object... args) {
        String msg;
        if (messageFormat != null && args != null && args.length > 0) {
            msg = String.format(messageFormat, args);
        } else {
            msg = messageFormat;
        }
        return msg;
    }


}