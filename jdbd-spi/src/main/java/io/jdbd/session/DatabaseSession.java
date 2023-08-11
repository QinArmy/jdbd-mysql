package io.jdbd.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.statement.*;
import org.reactivestreams.Publisher;

import java.util.Map;

/**
 * <p>
 * This interface representing database session.
 * </p>
 * <p>
 * This interface is is similar to {@code java.sql.Connection} or {@code javax.sql.XAConnection}, except that this interface is reactive.
 * </p>
 * <p>
 * The instance of this interface is created by {@link DatabaseSessionFactory}.
 * </p>
 * <p>
 * This interface extends {@link StaticStatementSpec} , so this interface can execute static statement without any statement option. eg: timeout.
 * </p>
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link LocalDatabaseSession}</li>
 *         <li>{@link RmDatabaseSession}</li>
 *         <li>{@link io.jdbd.pool.PoolLocalDatabaseSession}</li>
 *         <li>{@link io.jdbd.pool.PoolRmDatabaseSession}</li>
 *     </ul>
 * </p>
 * <p>
 *     Application developer can create statement by following methods :
 *     <ul>
 *         <li>{@link #statement()} ,create static statement.</li>
 *         <li>{@link #prepareStatement(String)} , create server-prepare statement</li>
 *         <li>{@link #bindStatement(String)} , create the adaptor of client-prepared statement and server-prepared statement.</li>
 *         <li>{@link #bindStatement(String, boolean)}, create the adaptor of client-prepared statement and server-prepared statement.</li>
 *         <li>{@link #multiStatement()}, create multi-statement</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface DatabaseSession extends StaticStatementSpec, DatabaseMetaSpec, Closeable {

    /**
     * @see DatabaseSessionFactory#name()
     */
    String factoryName();

    /**
     * <p>
     * Session identifier,probably is following :
     *     <ul>
     *         <li>process id</li>
     *         <li>thread id</li>
     *         <li>other identifier</li>
     *     </ul>
     *     <strong>NOTE</strong>: identifier will probably be updated if reconnect.
     * </p>
     *
     * @throws JdbdException throw when session have closed.
     */
    long identifier() throws JdbdException;

    /**
     * <p>
     * This method create one {@link DatabaseMetaData} instance.
     * </p>
     *
     * @throws JdbdException throw when session have closed.
     */
    DatabaseMetaData databaseMetaData() throws JdbdException;


    /**
     * <p>
     * <strong>NOTE</strong> : driver don't send message to database server before subscribing.
     * </p>
     */
    Publisher<TransactionStatus> transactionStatus();


    /**
     * <p>
     * Crete one static statement instance.
     * </p>
     * <p>
     * This method don't check session whether open or not.
     * </p>
     */
    StaticStatement statement();

    /**
     * <p>
     * This method create one server-prepared statement.
     * This method is similarly to {@code java.sql.Connection#prepareStatement(String)}
     * except that is async emit a {@link PreparedStatement}.
     * </p>
     * <p>
     * <strong>NOTE</strong> : driver don't send message to database server before subscribing.
     * </p>
     *
     * @return the {@link Publisher} emit just one {@link PreparedStatement} instance or {@link Throwable}. Like {@code reactor.core.publisher.Mono}
     * @throws JdbdException emit(not throw)
     *                       <ul>
     *                           <li>sql error</li>
     *                           <li>session have closed</li>
     *                           <li>database server response error message , see {@link io.jdbd.result.ServerException}</li>
     *                       </ul>
     */
    Publisher<PreparedStatement> prepareStatement(String sql) throws JdbdException;

    /**
     * <p>
     * This method is equivalent to following :
     * <pre>
     *         <code><br/>
     *             // session is instance of DatabaseSession
     *             session.bindStatement(sql,false) ;
     *         </code>
     *     </pre>
     * </p>
     *
     * @see #bindStatement(String, boolean)
     */
    BindStatement bindStatement(String sql);

    /**
     * <p>
     * Create the statement that is the adaptor of client-prepared statement and server-prepared statement.
     * </p>
     * <p>
     * This method don't check session whether open or not.
     * </p>
     *
     * @param sql                 have text sql.
     * @param forceServerPrepared true : must use server-prepared statement.
     * @throws IllegalArgumentException throw when only sql have no text.
     * @see BindStatement#isForcePrepare()
     */
    BindStatement bindStatement(String sql, boolean forceServerPrepared);

    /**
     * <p>
     * Create one multi statement.
     * </p>
     *
     * @throws JdbdException throw when :
     *                       <ul>
     *                           <li>session have closed</li>
     *                           <li>{@link #isSupportMultiStatement()} return false</li>
     *                       </ul>
     */
    MultiStatement multiStatement() throws JdbdException;

    /**
     * <p>
     * The state usually is returned database server by database client protocol.
     * For example :
     * <ul>
     *     <li>MySQL <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html">Protocol::OK_Packet</a> </li>
     *     <li>MySQL <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html">Protocol::EOF_Packet</a></li>
     *     <li>PostgreSQL <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">ReadyForQuery (B)</a></li>
     * </ul>
     * </p>
     *
     * @return true : this session in transaction block.
     * @throws JdbdException throw when session have closed.
     */
    boolean inTransaction() throws JdbdException;

    /**
     * <p>
     * <strong>NOTE</strong> : driver don't send message to database server before subscribing.
     * </p>
     */
    Publisher<SavePoint> setSavePoint();


    /**
     * <p>
     * <strong>NOTE</strong> : driver don't send message to database server before subscribing.
     * </p>
     */
    Publisher<SavePoint> setSavePoint(String name);

    Publisher<SavePoint> setSavePoint(String name, Map<Option<?>, ?> optionMap);


    Publisher<? extends DatabaseSession> releaseSavePoint(SavePoint savepoint);

    /**
     * <p>
     * <strong>NOTE</strong> : driver don't send message to database server before subscribing.
     * </p>
     *
     * @return the {@link Publisher} that completes successfully by
     * emitting an element(<strong>this</strong>), or with an error. Like {@code  reactor.core.publisher.Mono}
     */
    Publisher<? extends DatabaseSession> releaseSavePoint(SavePoint savepoint, Map<Option<?>, ?> optionMap);


    /**
     * <p>
     * <strong>NOTE</strong> : driver don't send message to database server before subscribing.
     * </p>
     *
     * @return the {@link Publisher} that completes successfully by
     * emitting an element(<strong>this</strong>), or with an error. Like {@code  reactor.core.publisher.Mono}
     */
    Publisher<? extends DatabaseSession> rollbackToSavePoint(SavePoint savepoint);

    Publisher<? extends DatabaseSession> rollbackToSavePoint(SavePoint savepoint, Map<Option<?>, ?> optionMap);


    DatabaseSession bindIdentifier(StringBuilder builder, String identifier);


    /**
     * @return true : session have closed.
     */
    boolean isClosed();

    /**
     * <p>
     * If return true , then the pool vendor developer must guarantee session and <strong>this</strong> both are created <br/>
     * by same pool {@link DatabaseSessionFactory} instance,and both underlying driver session instance are created by <br/>
     * same driver {@link DatabaseSessionFactory} instance.<br/>
     * </p>
     * <p>
     * This method can be useful , application developer can know session and <strong>this</strong> belong to
     * same resource manager in XA transaction.
     * </p>
     *
     * @return true : session and this both are created by same {@link DatabaseSessionFactory} instance.
     */
    boolean isSameFactory(DatabaseSession session);


    /**
     * <p>
     * This method should provide the access of some key(<strong>NOTE</strong> : is key,not all) properties of url ,but {@link io.jdbd.Driver#PASSWORD},
     * see {@link io.jdbd.Driver#createSessionFactory(String, Map)}.<br/>
     * </p>
     *
     * <p>
     * The implementation of this method must provide java doc(html list) for explaining supporting {@link Option} list.
     * </p>
     * <p>
     * The implementation of this method must support {@link Option#AUTO_RECONNECT}.
     * </p>
     * <p>
     * The implementation of this method perhaps support some of following :
     *     <ul>
     *         <li>{@link Option#AUTO_COMMIT}</li>
     *         <li>{@link Option#IN_TRANSACTION}</li>
     *         <li>{@link Option#READ_ONLY},true :  representing exists transaction and is read only.</li>
     *         <li>{@link Option#CLIENT_ZONE}</li>
     *         <li>{@link Option#SERVER_ZONE}</li>
     *         <li>{@link Option#CLIENT_CHARSET}</li>
     *         <li>{@link Option#BACKSLASH_ESCAPES}</li>
     *         <li>{@link Option#BINARY_HEX_ESCAPES}</li>
     *     </ul>
     * </p>
     *
     * @return null or the value of option.
     * @throws JdbdException throw when option need session open and session have closed.
     */
    @Nullable
    @Override
    <T> T valueOf(Option<T> option) throws JdbdException;


}
