package io.jdbd.session;

import io.jdbd.JdbdException;

public class SessionCloseException extends JdbdException {

    public static SessionCloseException create() {
        return new SessionCloseException("database session closed.");
    }

    public SessionCloseException(String messageFormat, Object... args) {
        super(messageFormat, args);
    }

    public SessionCloseException(Throwable cause, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
    }

    public SessionCloseException(Throwable cause, boolean enableSuppression, boolean writableStackTrace
            , String messageFormat, Object... args) {
        super(cause, enableSuppression, writableStackTrace, messageFormat, args);
    }

}
