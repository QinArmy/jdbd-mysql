package io.jdbd.mysql.protocol.client;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html">Capabilities Flags</a>
 */
abstract class Capabilities {

    // below Capabilities Flags https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
    static final byte CLIENT_LONG_PASSWORD = 1; /* new more secure passwords */
    static final byte CLIENT_FOUND_ROWS = 1 << 1;
    static final byte CLIENT_LONG_FLAG = 1 << 2; /* Get all column flags */
    static final byte CLIENT_CONNECT_WITH_DB = 1 << 3;
    static final byte CLIENT_COMPRESS = 1 << 5; /* Can use compression protcol */
    static final short CLIENT_LOCAL_FILES = 1 << 7; /* Can use LOAD DATA LOCAL */
    static final short CLIENT_PROTOCOL_41 = 1 << 9; // for > 4.1.1
    static final short CLIENT_INTERACTIVE = 1 << 10;
    static final short CLIENT_SSL = 1 << 11;
    static final short CLIENT_TRANSACTIONS = 1 << 13; // Client knows about transactions
    static final int CLIENT_SECURE_CONNECTION = 1 << 15;
    static final int CLIENT_MULTI_STATEMENTS = 1 << 16; // Enable/disable multiquery support
    static final int CLIENT_MULTI_RESULTS = 1 << 17; // Enable/disable multi-results
    static final int CLIENT_PS_MULTI_RESULTS = 1 << 18; // Enable/disable multi-results for server prepared statements
    static final int CLIENT_PLUGIN_AUTH = 1 << 19;
    static final int CLIENT_CONNECT_ATTRS = 1 << 20;
    static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;
    static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORD = 1 << 22;
    static final int CLIENT_SESSION_TRACK = 1 << 23;
    static final int CLIENT_DEPRECATE_EOF = 1 << 24;
    static final int CLIENT_OPTIONAL_RESULTSET_METADATA = 1 << 25;
    static final int CLIENT_QUERY_ATTRIBUTES = 1 << 27;
    static final int CLIENT_SSL_VERIFY_SERVER_CERT = 1 << 30;

    private Capabilities() {
        throw new UnsupportedOperationException();
    }

    static boolean supportSsl(final int negotiatedCapability) {
        return (negotiatedCapability & CLIENT_SSL) != 0;
    }

    static boolean supportMultiStatement(final int negotiatedCapability) {
        return (negotiatedCapability & CLIENT_MULTI_STATEMENTS) != 0;
    }

    static boolean supportQueryAttr(final int capability) {
        return (capability & CLIENT_QUERY_ATTRIBUTES) != 0;
    }


}
