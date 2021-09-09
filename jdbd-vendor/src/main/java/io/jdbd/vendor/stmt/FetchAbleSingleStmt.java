package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultStates;

import java.util.function.Consumer;

/**
 * <p>
 * This interface representing {@link SingleStmt} have only one sql and isn't batch,
 * so have below methods:
 *      <ul>
 *          <li>{@link #getFetchSize()}</li>
 *          <li>{@link #getStatusConsumer()}</li>
 *      </ul>
 *      The implementation of this interface is used by the implementation of below methods:
 *      <u>
 *          <li>{@link io.jdbd.stmt.StaticStatement#executeQuery(String)}</li>
 *          <li>{@link io.jdbd.stmt.StaticStatement#executeQuery(String, Consumer)}</li>
 *          <li>{@link io.jdbd.stmt.PreparedStatement#executeQuery()}</li>
 *          <li>{@link io.jdbd.stmt.PreparedStatement#executeQuery(Consumer)}</li>
 *          <li>{@link io.jdbd.stmt.BindStatement#executeQuery()}</li>
 *          <li>{@link io.jdbd.stmt.BindStatement#executeQuery(Consumer)}</li>
 *      </u>
 * </p>
 * <p>
 *     This interface is a base interface of :
 *     <ul>
 *         <li>{@link ParamStmt}</li>
 *         <li>{@link StaticStmt}</li>
 *     </ul>
 * </p>
 */
public interface FetchAbleSingleStmt extends SingleStmt {

    /**
     * @return negative or fetch size, if zero ignore.
     */
    int getFetchSize();

    Consumer<ResultStates> getStatusConsumer();


}