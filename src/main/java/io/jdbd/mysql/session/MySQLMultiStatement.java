package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.BatchQuery;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.statement.InOutParameter;
import io.jdbd.statement.MultiStatement;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.JdbdBinds;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.nio.file.Path;
import java.util.List;

import static io.jdbd.mysql.MySQLDriver.MY_SQL;

/**
 * <p>
 * This interface is a implementation of {@link MultiStatement} with MySQL client protocol.
 * </p>
 *
 * @since 1.0
 */
final class MySQLMultiStatement extends MySQLStatement<MultiStatement> implements MultiStatement {

    static MySQLMultiStatement create(MySQLDatabaseSession<?> session) {
        return new MySQLMultiStatement(session);
    }

    private MySQLMultiStatement(MySQLDatabaseSession<?> session) {
        super(session);
    }

    private final List<ParamStmt> stmtGroup = MySQLCollections.arrayList();

    private String currentSql;

    /**
     * current bind group.
     */
    private List<ParamValue> paramGroup = null;


    @Override
    public MultiStatement addStatement(final String sql) throws JdbdException {

        this.endStmtOption();

        final List<ParamValue> paramGroup = this.paramGroup;
        final RuntimeException error;

        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(MultiStatement.class);
        } else if (!MySQLStrings.hasText(sql)) {
            error = MySQLExceptions.sqlIsEmpty();
        } else if (paramGroup == null) {
            error = null;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(this.stmtGroup.size(), paramGroup);
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw MySQLExceptions.wrap(error);
        }

        final String lastSql = this.currentSql;
        if (lastSql != null) {
            this.stmtGroup.add(Stmts.paramStmt(lastSql, paramGroup, this));
        }

        this.currentSql = sql;
        this.paramGroup = null;
        return this;
    }


    @Override
    public MultiStatement bind(final int indexBasedZero, final @Nullable DataType dataType,
                               final @Nullable Object value) throws JdbdException {

        List<ParamValue> paramGroup = this.paramGroup;

        final MySQLType type;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(MultiStatement.class);
        } else if (indexBasedZero < 0) {
            error = MySQLExceptions.invalidParameterValue(this.stmtGroup.size(), indexBasedZero);
        } else if (value instanceof InOutParameter) {
            error = MySQLExceptions.dontSupportOutParameter(indexBasedZero, MultiStatement.class, MY_SQL);
        } else if (value instanceof Publisher || value instanceof Path) {
            error = MySQLExceptions.dontSupportJavaType(indexBasedZero, value, MY_SQL);
        } else if (dataType == null) {
            error = MySQLExceptions.dataTypeIsNull();
        } else if (value != null && (dataType == JdbdType.NULL || dataType == MySQLType.NULL)) {
            error = MySQLExceptions.nonNullBindValueOf(dataType);
        } else if ((type = MySQLBinds.mapDataType(dataType)) == null) {
            error = MySQLExceptions.dontSupportDataType(dataType, MY_SQL);
        } else {
            error = null;
            if (paramGroup == null) {
                this.paramGroup = paramGroup = MySQLCollections.arrayList();
            }
            paramGroup.add(JdbdValues.paramValue(indexBasedZero, type, value));
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw MySQLExceptions.wrap(error);
        }
        return this;
    }


    @Override
    public Flux<ResultStates> executeBatchUpdate() {

        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            return Flux.error(MySQLExceptions.cannotReuseStatement(MultiStatement.class));
        }

        this.endMultiStatement();

        final List<ParamStmt> stmtGroup = this.stmtGroup;
        final Flux<ResultStates> flux;
        if (stmtGroup.size() == 0) {
            flux = Flux.error(MySQLExceptions.multiStmtNoSql());
        } else {
            flux = this.session.protocol.multiStmtBatchUpdate(Stmts.paramMultiStmt(stmtGroup, this));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public BatchQuery executeBatchQuery() {
        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            return MultiResults.batchQueryError(MySQLExceptions.cannotReuseStatement(MultiStatement.class));
        }

        this.endMultiStatement();

        final List<ParamStmt> stmtGroup = this.stmtGroup;
        final BatchQuery batchQuery;
        if (stmtGroup.size() == 0) {
            batchQuery = MultiResults.batchQueryError(MySQLExceptions.multiStmtNoSql());
        } else {
            batchQuery = this.session.protocol.multiStmtBatchQuery(Stmts.paramMultiStmt(stmtGroup, this));
        }
        clearStatementToAvoidReuse();
        return batchQuery;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            return MultiResults.multiError(MySQLExceptions.cannotReuseStatement(MultiStatement.class));
        }

        this.endMultiStatement();

        final List<ParamStmt> stmtGroup = this.stmtGroup;
        final MultiResult multiResult;
        if (stmtGroup.size() == 0) {
            multiResult = MultiResults.multiError(MySQLExceptions.multiStmtNoSql());
        } else {
            multiResult = this.session.protocol.multiStmtAsMulti(Stmts.paramMultiStmt(stmtGroup, this));
        }
        clearStatementToAvoidReuse();
        return multiResult;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {

        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            return MultiResults.fluxError(MySQLExceptions.cannotReuseStatement(MultiStatement.class));
        }

        this.endMultiStatement();

        final List<ParamStmt> stmtGroup = this.stmtGroup;
        final OrderedFlux flux;
        if (stmtGroup.size() == 0) {
            flux = MultiResults.fluxError(MySQLExceptions.multiStmtNoSql());
        } else {
            flux = this.session.protocol.multiStmtAsFlux(Stmts.paramMultiStmt(stmtGroup, this));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    /*################################## blow Statement method ##################################*/

    @Override
    public boolean isSupportPublisher() {
        // always false,MySQL COM_QUERY protocol don't support Publisher
        return false;
    }

    @Override
    public boolean isSupportOutParameter() {
        // always false,MySQL COM_QUERY protocol don't support.
        return false;
    }

    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getName())
                .append("[ session : ")
                .append(this.session)
                .append(" , sqlList size : ")
                .append(this.stmtGroup.size())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }

    /*################################## blow MySQLStatement packet template method ##################################*/

    @Override
    void checkReuse() throws JdbdException {
        if (this.paramGroup == null && this.stmtGroup.size() > 0) {
            throw MySQLExceptions.cannotReuseStatement(MultiStatement.class);
        }
    }

    /*################################## blow private method ##################################*/

    private void clearStatementToAvoidReuse() {
        this.currentSql = null;
        this.paramGroup = EMPTY_PARAM_GROUP;
        this.stmtGroup.clear();
    }

    private void endMultiStatement() {
        final String lastSql = this.currentSql;
        if (lastSql != null) {
            this.stmtGroup.add(Stmts.paramStmt(lastSql, this.paramGroup, this));
        }
        this.currentSql = null;
        this.paramGroup = null;
    }


}
