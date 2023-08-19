package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.RmDatabaseSession;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.MultiStatement;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.StaticStatement;


/**
 * <p>
 * This interface is underlying api of below interfaces:
 *     <ul>
 *         <li>{@link LocalDatabaseSession}</li>
 *         <li>{@link RmDatabaseSession}</li>
 *         <li>{@link StaticStatement}</li>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 */
@Deprecated
public interface ClientProtocol0 extends MySQLProtocol {


}
