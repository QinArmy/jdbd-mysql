package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.session.DatabaseMetaSpec;
import io.jdbd.session.Option;
import io.jdbd.session.ServerVersion;

/**
 * <p>
 * This class is base class of following :
 *     <ul>
 *         <li>{@link MySQLDatabaseSession}</li>
 *         <li>{@link MySQLDatabaseMetadata}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
abstract class MySQLSessionMetaSpec implements DatabaseMetaSpec {

    final MySQLProtocol protocol;

    MySQLSessionMetaSpec(MySQLProtocol protocol) {
        this.protocol = protocol;
    }

    @Override
    public final ServerVersion serverVersion() throws JdbdException {
        return this.protocol.serverVersion();
    }

    @Override
    public final boolean isSupportSavePoints() throws JdbdException {
        //always true, MySQL support save point
        return true;
    }

    @Override
    public final boolean isSupportRefCursor() throws JdbdException {
        //always false,  MySQL don't support RefCurSor
        return false;
    }

    @Override
    public final boolean isSupportStoredProcedures() throws JdbdException {
        //always false,  MySQL support store procedures
        return false;
    }

    @Override
    public final boolean iSupportLocalTransaction() {
        return true;
    }

    @Override
    public final boolean isSupportStmtVar() throws JdbdException {
        return this.protocol.supportStmtVar();
    }

    @Override
    public final boolean isSupportMultiStatement() throws JdbdException {
        return this.protocol.supportMultiStmt();
    }

    @Override
    public final boolean isSupportOutParameter() throws JdbdException {
        return this.protocol.supportOutParameter();
    }


    /**
     * <p>
     * jdbd-mysql support following :
     *     <ul>
     *         <li>{@link Option#AUTO_COMMIT}</li>
     *         <li>{@link Option#IN_TRANSACTION}</li>
     *         <li>{@link Option#READ_ONLY},true :  representing exists transaction and is read only.</li>
     *         <li>{@link Option#CLIENT_ZONE}</li>
     *         <li>{@link Option#SERVER_ZONE} if support TRACK_SESSION_STATE enabled</li>
     *         <li>{@link Option#CLIENT_CHARSET}</li>
     *         <li>{@link Option#BACKSLASH_ESCAPES}</li>
     *         <li>{@link Option#BINARY_HEX_ESCAPES}</li>
     *         <li>{@link Option#AUTO_RECONNECT}</li>
     *     </ul>
     * </p>
     */
    @Override
    public final <T> T valueOf(Option<T> option) throws JdbdException {
        return this.protocol.valueOf(option);
    }

}
