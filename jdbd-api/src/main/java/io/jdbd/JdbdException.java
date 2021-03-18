package io.jdbd;


import reactor.util.annotation.Nullable;


/**
 * @see JdbdSQLException
 * @see JdbdNonSQLException
 */
public abstract class JdbdException extends RuntimeException {

    JdbdException(String message) {
        super(message);
    }

    JdbdException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }


    JdbdException(String message, @Nullable Throwable cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }


    JdbdException(@Nullable Throwable cause, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause);

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
