/*
 * Copyright 2023-2043 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.meta.ServerMode;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.session.DatabaseMetaSpec;
import io.jdbd.session.ServerVersion;

/**
 * <p>
 * This class is base class of following :
 *     <ul>
 *         <li>{@link MySQLDatabaseSession}</li>
 *         <li>{@link MySQLDatabaseMetadata}</li>
 *     </ul>
 * <br/>
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
        if (this.protocol.isClosed()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
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
        //always true,  MySQL support store procedures
        return true;
    }

    @Override
    public final boolean iSupportLocalTransaction() {
        return true;
    }

    @Override
    public final boolean isSupportStmtVar() throws JdbdException {
        if (this.protocol.isClosed()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
        return this.protocol.supportStmtVar();
    }

    @Override
    public final boolean isSupportMultiStatement() throws JdbdException {
        if (this.protocol.isClosed()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
        return this.protocol.supportMultiStmt();
    }

    @Override
    public final boolean isSupportOutParameter() throws JdbdException {
        if (this.protocol.isClosed()) {
            throw MySQLExceptions.sessionHaveClosed();
        }
        return this.protocol.supportOutParameter();
    }

    @Override
    public final boolean isSupportImportPublisher() throws JdbdException {
        //always false,MySQL don't support
        return false;
    }

    @Override
    public final boolean isSupportExportSubscriber() throws JdbdException {
        //always false,MySQL don't support
        return false;
    }

    @Override
    public final String factoryVendor() {
        return MySQLDriver.DRIVER_VENDOR;
    }

    @Override
    public final String driverVendor() {
        return MySQLDriver.DRIVER_VENDOR;
    }

    @Override
    public final ServerMode serverMode() throws JdbdException {
        return ServerMode.REMOTE;
    }

    @Override
    public final String supportProductFamily() {
        return MySQLDriver.MY_SQL;
    }

}
