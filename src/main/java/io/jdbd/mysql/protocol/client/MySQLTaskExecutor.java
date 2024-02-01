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

package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.SessionEnv;
import io.jdbd.mysql.env.MySQLHostInfo;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.syntax.DefaultMySQLParser;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.mysql.syntax.MySQLStatement;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.vendor.env.JdbdHost;
import io.jdbd.vendor.task.CommunicationTask;
import io.jdbd.vendor.task.CommunicationTaskExecutor;
import io.jdbd.vendor.util.JdbdSoftReference;
import io.jdbd.vendor.util.Pair;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Predicate;

final class MySQLTaskExecutor extends CommunicationTaskExecutor<TaskAdjutant> {


    static MySQLTaskExecutor create(final ClientProtocolFactory factory, final Connection connection) {
        return new MySQLTaskExecutor(connection, factory);
    }

    private static final Logger LOG = LoggerFactory.getLogger(MySQLTaskExecutor.class);

    private final ClientProtocolFactory factory;


    private MySQLTaskExecutor(Connection connection, ClientProtocolFactory factory) {
        super(connection, factory.factoryTaskQueueSize);
        this.factory = factory;
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected TaskAdjutant createTaskAdjutant() {
        return new MySQLTaskAdjutant(this);
    }


    protected void updateServerStatus(Object serversStatus) {
        ((MySQLTaskAdjutant) this.taskAdjutant).onTerminator((Terminator) serversStatus);
    }

    @Override
    protected JdbdHost obtainHostInfo() {
        return this.factory.mysqlHost;
    }

    @Override
    protected boolean clearChannel(ByteBuf cumulateBuffer, Class<? extends CommunicationTask> taskClass) {
        //TODO zoro complement this method.
        return true;
    }

    void setAuthenticateResult(AuthenticateResult result) {
        synchronized (this.taskAdjutant) {
            final MySQLTaskAdjutant adjutantWrapper = (MySQLTaskAdjutant) this.taskAdjutant;
            if (adjutantWrapper.handshake10 != null || adjutantWrapper.negotiatedCapability != 0) {
                throw new IllegalStateException("Duplicate update AuthenticateResult");
            }

            // 1.
            Handshake10 handshake = Objects.requireNonNull(result, "result").handshakeV10Packet();
            adjutantWrapper.handshake10 = Objects.requireNonNull(handshake, "handshake");

            //2.
            Charset serverCharset = Charsets.getJavaCharsetByCollationIndex(handshake.getCollationIndex());
            if (serverCharset == null) {
                throw new IllegalArgumentException("server handshake charset is null");
            }
            adjutantWrapper.serverHandshakeCharset = serverCharset;

            // 3.
            int negotiatedCapability = result.capability();
            if (negotiatedCapability == 0) {
                throw new IllegalArgumentException("result error.");
            }
            adjutantWrapper.negotiatedCapability = negotiatedCapability;


        }
    }

    void resetTaskAdjutant(final SessionEnv sessionEnv) {
        LOG.debug("reset success,server:{}", sessionEnv);
        synchronized (this.taskAdjutant) {
            MySQLTaskAdjutant taskAdjutant = (MySQLTaskAdjutant) this.taskAdjutant;
            // 1.
            taskAdjutant.sessionEnv = sessionEnv;
        }

    }

    @Override
    protected void onChannelClosed() {
        ((MySQLTaskAdjutant) this.taskAdjutant).onSessionClose();
    }


    Mono<Void> setCustomCollation(final Map<String, MyCharset> customCharsetMap,
                                  final Map<Integer, Collation> customCollationMap) {
        final Mono<Void> mono;
        final MySQLTaskAdjutant adjutant = ((MySQLTaskAdjutant) this.taskAdjutant);
        if (this.eventLoop.inEventLoop()) {
            adjutant.setCustomCharsetMap(customCharsetMap);
            adjutant.setIdCollationMap(customCollationMap);
            mono = Mono.empty();
        } else {
            mono = Mono.create(sink -> this.eventLoop.execute(() -> {
                adjutant.setCustomCharsetMap(customCharsetMap);
                adjutant.setIdCollationMap(customCollationMap);
                sink.success();
            }));
        }
        return mono;
    }




    /*################################## blow private method ##################################*/


    private static final class MySQLTaskAdjutant extends JdbdTaskAdjutant implements TaskAdjutant {

        private static final AtomicIntegerFieldUpdater<MySQLTaskAdjutant> SERVER_STATUS =
                AtomicIntegerFieldUpdater.newUpdater(MySQLTaskAdjutant.class, "serverStatus");


        private final MySQLTaskExecutor taskExecutor;

        private final Predicate<SQLMode> sqlModeFunc;

        private final BiFunction<String, JdbdSoftReference<MySQLStatement>, JdbdSoftReference<MySQLStatement>> stmtComputeFunc;

        private final MySQLParser stmtParser;

        private final List<Runnable> sessionCloseListenerList = MySQLCollections.arrayList(1);

        private final List<Runnable> transactionEndListenerList = MySQLCollections.arrayList(1);

        private Handshake10 handshake10;

        private Charset serverHandshakeCharset;

        private int negotiatedCapability = 0;


        private SessionEnv sessionEnv;

        private Map<String, MyCharset> customCharsetMap = Collections.emptyMap();

        private Map<Integer, Collation> idCollationMap = Collections.emptyMap();

        private Map<String, Collation> nameCollationMap = Collections.emptyMap();

        private volatile int serverStatus = 0;


        private MySQLTaskAdjutant(MySQLTaskExecutor taskExecutor) {
            super(taskExecutor);
            this.taskExecutor = taskExecutor;
            this.sqlModeFunc = this::containSQLMode;
            this.stmtComputeFunc = this::computeStmt;
            this.stmtParser = DefaultMySQLParser.create(this.sqlModeFunc);
        }


        @Override
        public ClientProtocolFactory getFactory() {
            return this.taskExecutor.factory;
        }


        @Override
        public Charset charsetClient() {
            SessionEnv server = this.sessionEnv;
            return server == null ? StandardCharsets.UTF_8 : server.charsetClient();
        }

        /**
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html#charset-connection-client-configuration">Client Program Connection Character Set Configuration</a>
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_character_set_results">character_set_results</a>
         */
        @Nullable
        @Override
        public Charset getCharsetResults() {
            SessionEnv server = this.sessionEnv;
            Charset charset;
            if (server == null) {
                charset = StandardCharsets.UTF_8;
            } else {
                charset = server.charsetResults();
            }
            return charset;
        }

        @Override
        public Charset columnCharset(Charset columnCharset) {
            Charset charset = getCharsetResults();
            if (charset == null) {
                charset = columnCharset;
            }
            return charset;
        }

        /**
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-errors.html">Error Message Character Set</a>
         */
        @Override
        public Charset errorCharset() {
            Charset errorCharset = getCharsetResults();
            if (errorCharset == null) {
                errorCharset = StandardCharsets.UTF_8;
            }
            return errorCharset;
        }

        @Override
        public Charset obtainCharsetMeta() {
            Charset metaCharset = getCharsetResults();
            if (metaCharset == null) {
                metaCharset = StandardCharsets.UTF_8;
            }
            return metaCharset;
        }

        @Override
        public int capability() {
            // here , don't check session whether open or not
            int capacity = this.negotiatedCapability;
            if (capacity == 0) {
                LOG.trace("Cannot access negotiatedCapability[{}],this[{}]", this.negotiatedCapability, this);
                throw new IllegalStateException("Cannot access negotiatedCapability now.");
            }
            return capacity;
        }

        @Override
        public Map<Integer, CustomCollation> obtainCustomCollationMap() {
            return Collections.emptyMap();
        }

        @Override
        public ZoneOffset serverZone() {
            SessionEnv server = this.sessionEnv;
            if (server == null) {
                // before reset .
                return MySQLTimes.systemZoneOffset();
            }
            return server.serverZone();
        }

        @Override
        public Handshake10 handshake10() {
            // here , don't check session whether open or not
            Handshake10 packet = this.handshake10;
            if (packet == null) {
                throw new IllegalStateException("Cannot access handshakeV10Packet now.");
            }
            return packet;
        }

        @Override
        public MySQLHostInfo host() {
            return this.taskExecutor.factory.mysqlHost;
        }


        @Override
        public ZoneOffset connZone() {
            SessionEnv server = this.sessionEnv;
            if (server == null) {
                throw new IllegalStateException("Cannot access zoneOffsetClient now.");
            }
            return server.serverZone();
        }

        @Override
        public int serverStatus() {
            return this.serverStatus;
        }


        @Override
        public boolean isAuthenticated() {

            return this.handshake10 != null;
        }


        @Override
        public Map<String, Charset> customCharsetMap() {
            return this.taskExecutor.factory.customCharsetMap;
        }

        @Override
        public Map<String, MyCharset> nameCharsetMap() {
            final Map<String, MyCharset> map = this.customCharsetMap;
            if (map == null) {
                throw new IllegalStateException("this.customCharsetMap is null.");
            }
            return map;
        }

        @Override
        public Map<Integer, Collation> idCollationMap() {
            final Map<Integer, Collation> map = this.idCollationMap;
            if (map == null) {
                throw new IllegalStateException("this.customCollationMap is null.");
            }
            return map;
        }

        @Override
        public Map<String, Collation> nameCollationMap() {
            final Map<String, Collation> map = this.nameCollationMap;
            if (map == null) {
                throw new IllegalStateException("this.nameCollationMap is null.");
            }
            return map;
        }


        @Override
        public SessionEnv sessionEnv() {
            SessionEnv server = this.sessionEnv;
            if (server == null) {
                throw new IllegalStateException("Cannot access server now.");
            }
            return server;
        }


        @Override
        public MySQLStatement parse(final String singleSql) throws JdbdException {
            if (singleSql.length() > (1 << 16)) { // TODO optimizing me
                return this.stmtParser.parse(singleSql);
            }

            final ConcurrentMap<String, JdbdSoftReference<MySQLStatement>> stmtMap;
            stmtMap = this.taskExecutor.factory.statementMap; // TODO optimizing me

            JdbdSoftReference<MySQLStatement> ref;
            ref = stmtMap.compute(singleSql, this.stmtComputeFunc);

            MySQLStatement statement;
            statement = ref.get();
            if (statement == null) {
                this.taskExecutor.factory.clearInvalidStmtSoftRef();
                statement = this.stmtParser.parse(singleSql);
                stmtMap.put(singleSql, JdbdSoftReference.reference(statement));
            }
            return statement;
        }


        @Override
        public boolean isSingleStmt(final String sql) throws JdbdException {
            return this.taskExecutor.factory.statementMap.get(sql) != null || this.stmtParser.isSingleStmt(sql);
        }

        @Override
        public boolean isMultiStmt(String sql) throws JdbdException {
            return this.stmtParser.isMultiStmt(sql);
        }


        @Override
        public void addSessionCloseListener(final Runnable listener) {
            if (this.taskExecutor.eventLoop.inEventLoop()) {
                this.sessionCloseListenerList.add(listener);
            } else {
                this.taskExecutor.eventLoop.execute(() -> this.sessionCloseListenerList.add(listener));
            }
        }

        @Override
        public void addTransactionEndListener(final Runnable listener) {
            if (this.taskExecutor.eventLoop.inEventLoop()) {
                this.transactionEndListenerList.add(listener);
            } else {
                this.taskExecutor.eventLoop.execute(() -> this.transactionEndListenerList.add(listener));
            }
        }

        private void setCustomCharsetMap(Map<String, MyCharset> customCharsetMap) {
            this.customCharsetMap = customCharsetMap;
        }

        private void setIdCollationMap(final Map<Integer, Collation> idCollationMap) {
            final Map<String, Collation> nameCollationMap = MySQLCollections.hashMap((int) (idCollationMap.size() / 0.75F));
            for (Collation collation : idCollationMap.values()) {
                nameCollationMap.put(collation.name, collation);
            }
            this.nameCollationMap = nameCollationMap;
            this.idCollationMap = idCollationMap;
        }

        /**
         * <p>
         * This method always run in {@link io.netty.channel.EventLoop}
         * <br/>
         *
         * @see MySQLTaskExecutor#updateServerStatus(Object)
         */
        private void onTerminator(final Terminator terminator) {
            final int oldServerStatus = this.serverStatus;
            if (Terminator.inTransaction(oldServerStatus) && !Terminator.inTransaction(terminator.statusFags)) {
                for (Runnable listener : this.transactionEndListenerList) {
                    listener.run(); // don't throw error
                }
            }
            SERVER_STATUS.set(this, terminator.statusFags);

            final OkPacket.StateOption stateOption;
            if (this.sessionEnv != null
                    && terminator instanceof OkPacket
                    && (stateOption = ((OkPacket) terminator).stateOption) != null
                    && stateOption.variablePairList.size() > 0) {
                handleSessionVariableChanged(stateOption.variablePairList);
            }

        }

        private void handleSessionVariableChanged(final List<Pair<String, String>> list) {
            final int varPairSize;
            varPairSize = list.size();

            Pair<String, String> pair;
            ZoneOffset serverZone = null;
            Charset resultCharset = null;
            Charset clientCharset = null;
            String sqlModeStr = null;
            boolean serverZoneChanged = false;
            for (int i = 0; i < varPairSize; i++) {
                pair = list.get(i);
                switch (pair.getFirst().toLowerCase(Locale.ROOT)) {
                    case "time_zone":
                        serverZoneChanged = true;
                        serverZone = parseServerZone(pair.getSecond());
                        break;
                    case "character_set_results":
                        resultCharset = Charsets.getJavaCharsetByCharsetName(pair.getSecond());
                        break;
                    case "character_set_client":
                        clientCharset = Charsets.getJavaCharsetByCharsetName(pair.getSecond());
                        break;
                    case "sql_mode":
                        sqlModeStr = pair.getSecond();
                        break;
                    default:
                        // no-op
                }
            }// for loop

            final SessionEnv sessionEnv = this.sessionEnv;
            if (sessionEnv != null) {
                LOG.debug("update sessionEnv ,serverZone {} ,resultCharset : {} ,clientCharset : {} ,sqlModeStr : {}",
                        serverZone, resultCharset, clientCharset, sqlModeStr);
                this.sessionEnv = ClientProtocolFactory.updateSessionEnvIfNeed(sessionEnv, serverZone, resultCharset, clientCharset, sqlModeStr);
            }

            if (serverZoneChanged && serverZone == null) {
                // TODO add  urgency task
            }

        }

        @Nullable
        private static ZoneOffset parseServerZone(final String zoneStr) {
            if (Constants.SERVER.equals(zoneStr)) {
                return null;
            }
            ZoneOffset serverZone;
            try {
                serverZone = ZoneOffset.of(zoneStr);
            } catch (Throwable e) {
                try {
                    serverZone = ZoneId.of(zoneStr, ZoneId.SHORT_IDS).getRules().getOffset(Instant.EPOCH);
                } catch (Throwable ex) {
                    serverZone = null;
                }
            }
            return serverZone;
        }


        /**
         * @see MySQLTaskExecutor#onChannelClosed()
         */
        private void onSessionClose() {
            for (Runnable listener : this.sessionCloseListenerList) {
                listener.run(); // don't throw error
            }
        }

        /**
         * <p>
         * Just for {@link #stmtParser}
         * <br/>
         */
        private boolean containSQLMode(final SQLMode mode) {
            boolean match;
            if (mode == SQLMode.NO_BACKSLASH_ESCAPES) {
                match = Terminator.isNoBackslashEscapes(this.serverStatus); // always exactly, @see updateServerStatus(Terminator)
            } else {
                final SessionEnv sessionEnv = this.sessionEnv;
                match = sessionEnv != null && sessionEnv.containSqlMode(mode);
            }
            return match;
        }

        /**
         * @see #parse(String)
         */
        private JdbdSoftReference<MySQLStatement> computeStmt(final String sql, final @Nullable JdbdSoftReference<MySQLStatement> oldRef) {
            MySQLStatement statement;

            final JdbdSoftReference<MySQLStatement> newRef;
            if (oldRef == null || (statement = oldRef.get()) == null || statement.isInvalid(this.sqlModeFunc)) {
                statement = this.stmtParser.parse(sql);
                newRef = JdbdSoftReference.reference(statement);
            } else {
                newRef = oldRef;
            }
            if (oldRef != null && newRef != oldRef) {
                this.taskExecutor.factory.clearInvalidStmtSoftRef();
            }
            return newRef;
        }


    }// MySQLTaskAdjutant


}
