package io.jdbd.mysql.session;

import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.*;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.Stmts;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * <p>
 * This interface is a implementation of {@link StaticStatement} with MySQL protocol.
 * </p>
 *
 * @since 1.0
 */
final class MySQLStaticStatement extends MySQLStatement<StaticStatement> implements StaticStatement {


    static MySQLStaticStatement create(final MySQLDatabaseSession<?> session) {
        return new MySQLStaticStatement(session);
    }

    private MySQLStaticStatement(final MySQLDatabaseSession<?> session) {
        super(session);
    }


    @Override
    public Publisher<ResultStates> executeUpdate(final String sql) {
        this.endStmtOption();

        if (!MySQLStrings.hasText(sql)) {
            return Mono.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.update(Stmts.stmt(sql, this));
    }

    @Override
    public Publisher<ResultRow> executeQuery(String sql) {
        return this.executeQuery(sql, CurrentRow::asResultRow, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(String sql, Function<CurrentRow, R> function) {
        return this.executeQuery(sql, function, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(final String sql, final Function<CurrentRow, R> function,
                                         final Consumer<ResultStates> consumer) {
        this.endStmtOption();

        if (!MySQLStrings.hasText(sql)) {
            return Flux.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.query(Stmts.stmt(sql, consumer, this), function);
    }

    @Override
    public Publisher<ResultStates> executeBatchUpdate(final List<String> sqlGroup) {
        this.endStmtOption();

        if (MySQLCollections.isEmpty(sqlGroup)) {
            return Flux.error(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.batchUpdate(Stmts.batch(sqlGroup, this));
    }

    @Override
    public BatchQuery executeBatchQuery(final List<String> sqlGroup) {
        this.endStmtOption();

        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.batchQueryError(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.batchQuery(Stmts.batch(sqlGroup));
    }


    @Override
    public MultiResult executeBatchAsMulti(final List<String> sqlGroup) {
        this.endStmtOption();

        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.multiError(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.batchAsMulti(Stmts.batch(sqlGroup, this));
    }

    @Override
    public OrderedFlux executeBatchAsFlux(final List<String> sqlGroup) {
        this.endStmtOption();

        if (MySQLCollections.isEmpty(sqlGroup)) {
            return MultiResults.fluxError(MySQLExceptions.sqlIsEmpty());
        }
        return this.session.protocol.batchAsFlux(Stmts.batch(sqlGroup, this));
    }


    @Override
    public OrderedFlux executeAsFlux(final String multiStmt) {
        this.endStmtOption();

        final OrderedFlux flux;
        if (!this.session.protocol.supportMultiStmt()) {
            flux = MultiResults.fluxError(MySQLExceptions.dontSupportMultiStmt());
        } else if (MySQLStrings.hasText(multiStmt)) {
            flux = this.session.protocol.executeAsFlux(Stmts.multiStmt(multiStmt, this));
        } else {
            flux = MultiResults.fluxError(MySQLExceptions.sqlIsEmpty());
        }
        return flux;
    }

    @Override
    public Publisher<RefCursor> declareCursor(String sql) {
        return Mono.error(MySQLExceptions.dontSupportDeclareCursor(MySQLDriver.MY_SQL));
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
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


    /*################################## blow Statement packet template method ##################################*/

    @Override
    void checkReuse() {
        //no-op
    }

    /*################################## blow private static method ##################################*/


}
