package io.jdbd.mysql.protocol;

import io.jdbd.JdbdException;

/**
 * emit when server response unrecognized collation index.
 *
 * @since 1.0
 */
public final class UnrecognizedCollationException extends JdbdException {


    public UnrecognizedCollationException(String message) {
        super(message);
    }


}
