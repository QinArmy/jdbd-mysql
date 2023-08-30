package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.session.ChunkOption;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.statement.BindSingleStatement;
import io.jdbd.statement.Parameter;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.Statement;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.NamedValue;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.StmtOption;
import io.jdbd.vendor.util.JdbdStrings;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.jdbd.mysql.MySQLDriver.MY_SQL;


/**
 * <p>
 * This interface is a implementation of {@link Statement} with MySQL client protocol.
 * </p>
 * <p>
 * This class is base class of following :
 *     <ul>
 *         <li>{@link MySQLStaticStatement}</li>
 *         <li>{@link MySQLPreparedStatement}</li>
 *         <li>{@link MySQLBindStatement}</li>
 *         <li>{@link MySQLMultiStatement}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
abstract class MySQLStatement<S extends Statement> implements Statement, StmtOption {


    static final List<ParamValue> EMPTY_PARAM_GROUP = Collections.emptyList();

    final MySQLDatabaseSession<?> session;

    private int timeoutMillSeconds;

    int fetchSize;

    private Map<String, NamedValue> queryAttrMap;


    MySQLStatement(MySQLDatabaseSession<?> session) {
        this.session = session;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/query-attributes.html">Query Attributes</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY , static statement Query Attributes bind</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE , prepared statement Query Attributes bind</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final S bindStmtVar(final String name, final @Nullable DataType dataType,
                               final @Nullable Object value) throws JdbdException {

        RuntimeException error = null;
        final MySQLType type;
        if (!JdbdStrings.hasText(name)) {
            error = MySQLExceptions.stmtVarNameHaveNoText(name);
        } else if (dataType == null) {
            error = MySQLExceptions.dataTypeIsNull();
        } else if (value instanceof Parameter) {
            error = MySQLExceptions.dontSupportJavaType(name, value, MY_SQL);
        } else if (value != null && (dataType == JdbdType.NULL || dataType == MySQLType.NULL)) {
            error = MySQLExceptions.nonNullBindValueOf(dataType);
        } else if ((type = MySQLBinds.mapDataType(dataType)) == null) {
            error = MySQLExceptions.dontSupportDataType(dataType, MY_SQL);
        } else {
            Map<String, NamedValue> map = this.queryAttrMap;
            if (map == null) {
                this.queryAttrMap = map = MySQLCollections.hashMap();
            } else if (!(map instanceof HashMap)) {
                // here,have closed
                throw MySQLExceptions.cannotReuseStatement(getClass());
            }

            if (map.putIfAbsent(name, JdbdValues.namedValue(name, type, value)) != null) {
                error = MySQLExceptions.stmtVarDuplication(name);
            }
        }

        if (error != null) {
            if (this instanceof PreparedStatement) {
                closeOnBindError(error);
            }
            throw error;
        }
        return (S) this;
    }

    @Override
    public final DatabaseSession getSession() {
        return this.session;
    }

    @Override
    public final <T extends DatabaseSession> T getSession(final Class<T> sessionClass) {
        try {
            return sessionClass.cast(this.session);
        } catch (Throwable e) {
            if (this instanceof PreparedStatement) {
                closeOnBindError(e);
            }
            throw MySQLExceptions.wrap(e);
        }
    }


    @Override
    public final boolean isSupportStmtVar() {
        return this.session.isSupportStmtVar();
    }

    @Override
    public final boolean isSupportPublisher() {
        return this instanceof BindSingleStatement;
    }

    @Override
    public final boolean isSupportPath() {
        return this instanceof BindSingleStatement;
    }

    @Override
    public final boolean isSupportOutParameter() {
        return this instanceof BindSingleStatement;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final S setTimeout(final int millSeconds) {
        if (millSeconds < 0) {
            final IllegalArgumentException error;
            error = MySQLExceptions.timeoutIsNegative(millSeconds);
            if (this instanceof PreparedStatement) {
                closeOnBindError(error);
            }
            throw error;
        }
        this.timeoutMillSeconds = millSeconds;
        return (S) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final S setFetchSize(final int fetchSize) {
        if (fetchSize < 0) {
            final IllegalArgumentException error;
            error = MySQLExceptions.fetchSizeIsNegative(fetchSize);
            if (this instanceof PreparedStatement) {
                closeOnBindError(error);
            }
            throw error;
        }
        if (this instanceof BindSingleStatement) {
            this.fetchSize = fetchSize;
        }
        return (S) this;
    }

    @Override
    public final S setImportPublisher(Function<ChunkOption, Publisher<byte[]>> function) throws JdbdException {
        final JdbdException error;
        error = MySQLExceptions.dontSupportImporter(MY_SQL);
        if (this instanceof PreparedStatement) {
            closeOnBindError(error);
        }
        throw error;
    }

    @Override
    public final S setExportSubscriber(Function<ChunkOption, Subscriber<byte[]>> function) throws JdbdException {
        final JdbdException error;
        error = MySQLExceptions.dontSupportExporter(MY_SQL);
        if (this instanceof PreparedStatement) {
            closeOnBindError(error);
        }
        throw error;
    }


    @Override
    public final <T> S setOption(Option<T> option, @Nullable T value) throws JdbdException {
        final JdbdException error;
        error = MySQLExceptions.dontSupportSetOption(option);
        if (this instanceof PreparedStatement) {
            closeOnBindError(error);
        }
        throw error;
    }

    @Override
    public final List<Option<?>> supportedOptionList() {
        return Collections.emptyList();
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
     *     </ul>
     * </p>
     */
    @Override
    public final <T> T valueOf(Option<T> option) {
        return this.session.protocol.valueOf(option);
    }


    @Override
    public final int getTimeout() {
        return this.timeoutMillSeconds;
    }

    @Override
    public final int getFetchSize() {
        return this.fetchSize;
    }

    @Override
    public final List<NamedValue> getStmtVarList() {
        final Map<String, NamedValue> map = this.queryAttrMap;
        List<NamedValue> list;
        if (map == null || map.size() == 0) {
            list = Collections.emptyList();
        } else {
            list = MySQLCollections.arrayList(map.size());
            list.addAll(map.values());
            list = MySQLCollections.unmodifiableList(list);
        }
        return list;
    }

    @Override
    public final Function<ChunkOption, Publisher<byte[]>> getImportFunction() {
        // always null
        return null;
    }

    @Override
    public final Function<ChunkOption, Subscriber<byte[]>> getExportFunction() {
        // always null
        return null;
    }

    @Override
    public final DatabaseSession databaseSession() {
        return this.session;
    }

    @Override
    public final int hashCode() {
        return super.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        return obj == this;
    }


    final void endStmtOption(final boolean clearFetchSize) {
        final Map<String, NamedValue> map = this.queryAttrMap;
        if (map == null) {
            this.queryAttrMap = Collections.emptyMap();
        } else if (map instanceof HashMap) {
            this.queryAttrMap = MySQLCollections.unmodifiableMap(map);
        }

        if (clearFetchSize) {
            this.fetchSize = 0;
        }
    }


    /**
     * @see MySQLPreparedStatement
     */
    void closeOnBindError(Throwable error) {
        // no-op
    }


}
