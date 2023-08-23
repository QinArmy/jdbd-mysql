package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.SessionEnv;
import io.jdbd.mysql.env.MySQLHost;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.session.MySQLDatabaseSession;
import io.jdbd.mysql.syntax.DefaultMySQLParser;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.mysql.syntax.MySQLStatement;
import io.jdbd.mysql.util.MySQLBuffers;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.ResultItem;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.vendor.env.JdbdHost;
import io.jdbd.vendor.session.JdbdTransactionStatus;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.task.CommunicationTask;
import io.jdbd.vendor.task.CommunicationTaskExecutor;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

final class MySQLTaskExecutor extends CommunicationTaskExecutor<TaskAdjutant> {


    static Mono<MySQLTaskExecutor> create(final ClientProtocolFactory factory) {

        return factory.tcpClient
                .connect()
                .map(connection -> new MySQLTaskExecutor(connection, factory));
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
        ((MySQLTaskAdjutant) this.taskAdjutant).updateServerStatus((Terminator) serversStatus);
    }

    @Override
    protected JdbdHost obtainHostInfo() {
        return this.factory.host;
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

    Mono<Void> reConnect(Duration duration) {
        return Mono.empty();
    }


    Mono<Void> setCustomCollation(final Map<String, MyCharset> customCharsetMap
            , final Map<Integer, Collation> customCollationMap) {
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


    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    private static void xidToString(final StringBuilder cmdBuilder, final Xid xid) throws XaException {
        final String gtrid, bqual;
        gtrid = xid.getGtrid();
        bqual = xid.getBqual();

        final byte[] gtridBytes, bqualBytes;

        if (!MySQLStrings.hasText(gtrid)) {
            throw MySQLExceptions.xaGtridNoText();
        } else if ((gtridBytes = gtrid.getBytes(StandardCharsets.UTF_8)).length > 64) {
            throw MySQLExceptions.xaGtridBeyond64Bytes();
        }

        cmdBuilder.append(" 0x")
                .append(MySQLBuffers.hexEscapesText(true, gtridBytes, gtridBytes.length));

        cmdBuilder.append(',');
        if (bqual != null) {
            if ((bqualBytes = bqual.getBytes(StandardCharsets.UTF_8)).length > 64) {
                throw MySQLExceptions.xaBqualBeyond64Bytes();
            }
            cmdBuilder.append("0x")
                    .append(MySQLBuffers.hexEscapesText(true, bqualBytes, bqualBytes.length));
        }

        cmdBuilder.append(',')
                .append(Integer.toUnsignedString(xid.getFormatId()));

    }


    private static final class MySQLTaskAdjutant extends JdbdTaskAdjutant
            implements TaskAdjutant, TransactionController {

        private static final AtomicIntegerFieldUpdater<MySQLTaskAdjutant> SERVER_STATUS =
                AtomicIntegerFieldUpdater.newUpdater(MySQLTaskAdjutant.class, "serverStatus");


        private final MySQLTaskExecutor taskExecutor;

        private final MySQLParser stmtParser;

        private Handshake10 handshake10;

        private Charset serverHandshakeCharset;

        private int negotiatedCapability = 0;


        private SessionEnv sessionEnv;

        private Map<String, MyCharset> customCharsetMap = Collections.emptyMap();

        private Map<Integer, Collation> idCollationMap = Collections.emptyMap();

        private Map<String, Collation> nameCollationMap = Collections.emptyMap();

        private volatile int serverStatus = 0;

        /**
         * <p>
         *     <ul>
         *         <li>c</li>
         *     </ul>
         * </p>
         */
        private volatile CurrentTxOption currentTxOption;

        private MySQLTaskAdjutant(MySQLTaskExecutor taskExecutor) {
            super(taskExecutor);
            this.taskExecutor = taskExecutor;
            this.stmtParser = DefaultMySQLParser.create(this::containSQLMode);
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
            int capacity = this.negotiatedCapability;
            if (capacity == 0) {
                LOG.trace("Cannot access negotiatedCapability[{}],this[{}]", this.negotiatedCapability, this);
                throw new IllegalStateException("Cannot access negotiatedCapability now.");
            }
            return capacity;
        }

        @Override
        public Map<Integer, Charsets.CustomCollation> obtainCustomCollationMap() {
            return Collections.emptyMap();
        }

        @Override
        public ZoneOffset serverZone() {
            SessionEnv server = this.sessionEnv;
            if (server == null) {
                throw new JdbdException("Cannot access zoneOffsetDatabase now.");
            }
            return server.connZone();
        }

        @Override
        public Handshake10 handshake10() {
            Handshake10 packet = this.handshake10;
            if (packet == null) {
                throw new IllegalStateException("Cannot access handshakeV10Packet now.");
            }
            return packet;
        }

        @Override
        public MySQLHost host() {
            return this.taskExecutor.factory.host;
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
        public boolean inTransaction() {
            if (!this.taskExecutor.connection.channel().isActive()) {
                throw new SessionCloseException("session have closed");
            }
            return Terminator.inTransaction(this.serverStatus);
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
        public MySQLStatement parse(String singleSql) throws JdbdException {
            MySQLParser parser = this.stmtParser;
            if (parser == null) {
                throw new IllegalStateException("Cannot access MySQLParser now.");
            }
            return parser.parse(singleSql);
        }

        @Override
        public boolean isSingleStmt(String sql) throws JdbdException {
            MySQLParser parser = this.stmtParser;
            if (parser == null) {
                throw new IllegalStateException("Cannot access MySQLParser now.");
            }
            return parser.isSingleStmt(sql);
        }

        @Override
        public boolean isMultiStmt(String sql) throws JdbdException {
            MySQLParser parser = this.stmtParser;
            if (parser == null) {
                throw new IllegalStateException("Cannot access MySQLParser now.");
            }
            return parser.isMultiStmt(sql);
        }


        @Override
        public Mono<ResultStates> startTransaction(final @Nullable TransactionOption option, final @Nullable HandleMode mode) {
            return Mono.empty();
        }

        @Override
        public Mono<TransactionStatus> transactionStatus() {
            final MySQLServerVersion version = this.handshake10.serverVersion;
            final StringBuilder builder = new StringBuilder(139);
            if (version.meetsMinimum(8, 0, 3)
                    || (version.meetsMinimum(5, 7, 20) && !version.meetsMinimum(8, 0, 0))) {
                builder.append("SELECT @@session.transaction_isolation AS txLevel")
                        .append(",@@session.transaction_read_only AS txReadOnly");
            } else {
                builder.append("SELECT @@session.tx_isolation AS txLevel")
                        .append(",@@session.tx_read_only AS txReadOnly");
            }

            return Flux.from(ComQueryTask.executeAsFlux(Stmts.multiStmt(builder.toString()), this))
                    .filter(ResultItem::isRowOrStatesItem)
                    .collectList()
                    .flatMap(this::mapTransactionStatus);

        }


        @Override
        public Mono<ResultStates> start(final @Nullable Xid xid, final int flags, final @Nullable TransactionOption option) {
            final StringBuilder builder = new StringBuilder(140);
            final Isolation isolation;
            try {
                final CurrentTxOption currentTxOption = this.currentTxOption;
                if (xid == null) {
                    throw MySQLExceptions.xidIsNull();
                } else if (option == null) {
                    throw MySQLExceptions.xaTransactionOptionIsNull();
                } else if (currentTxOption != null || this.inTransaction()) {
                    throw MySQLExceptions.xaBusyOnOtherTransaction();
                }
                builder.append("SET TRANSACTION ");
                isolation = option.isolation();
                if (isolation != null) {
                    builder.append("ISOLATION LEVEL ");
                    if (MySQLDatabaseSession.appendIsolation(isolation, builder)) {
                        throw MySQLExceptions.unknownIsolation(isolation);
                    }
                    builder.append(Constants.SPACE_COMMA_SPACE);
                }

                if (option.isReadOnly()) {
                    builder.append("READ ONLY");
                } else {
                    builder.append("READ WRITE");
                }

                builder.append(Constants.SPACE_SEMICOLON_SPACE)
                        .append("XA START");

                xidToString(builder, xid);
                switch (flags) {
                    case RmDatabaseSession.TM_JOIN:
                        builder.append(" JOIN");
                        break;
                    case RmDatabaseSession.TM_RESUME:
                        builder.append(" RESUME");
                        break;
                    case RmDatabaseSession.TM_NO_FLAGS:
                        // no-op
                        break;
                    default:
                        throw JdbdExceptions.xaInvalidFlagForStart(flags);
                }
            } catch (Throwable e) {
                return Mono.error(MySQLExceptions.wrap(e));
            }
            return Flux.from(ComQueryTask.executeAsFlux(Stmts.multiStmt(builder.toString()), this))
                    .last()
                    .map(ResultStates.class::cast)
                    .doOnSuccess(states -> {
                        if (states.inTransaction()) {
                            CURRENT_TX_OPTION.set(this, new XaTxOption(isolation, xid, XaStates.ACTIVE, flags));
                        } else {
                            CURRENT_TX_OPTION.set(this, null);
                        }
                    }).doOnError(e -> CURRENT_TX_OPTION.set(this, null));
        }

        @Override
        public Mono<ResultStates> end(Xid xid, int flags, Function<Option<?>, ?> optionFunc) {
            return null;
        }

        @Override
        public Mono<Integer> prepare(Xid xid, Function<Option<?>, ?> optionFunc) {
            return null;
        }

        @Override
        public Mono<ResultStates> commit(Xid xid, int flags, Function<Option<?>, ?> optionFunc) {
            return null;
        }

        @Override
        public Mono<RmDatabaseSession> rollback(Xid xid, Function<Option<?>, ?> optionFunc) {
            return null;
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
         * Just for {@link #stmtParser}
         * </p>
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

        private void updateServerStatus(final Terminator terminator) {
            SERVER_STATUS.set(this, terminator.statusFags);
            //TODO UPDATE session track
        }




        /**
         * @see #transactionStatus()
         */
        private Mono<TransactionStatus> mapTransactionStatus(final List<ResultItem> list) {
            final ResultRow row;
            final ResultStates states;
            row = (ResultRow) list.get(0);
            states = (ResultStates) list.get(1);


            final Boolean readOnly;
            final CurrentTxOption currentTxOption;

            final Mono<TransactionStatus> mono;
            if ((readOnly = states.valueOf(Option.READ_ONLY)) == null) {
                // no bug,never here
                mono = Mono.error(new JdbdException("result status no read only"));
            } else if (!states.inTransaction()) {
                // session transaction characteristic
                final Isolation isolation;
                isolation = row.getNonNull(0, Isolation.class);
                mono = Mono.just(JdbdTransactionStatus.txStatus(isolation, row.getNonNull(1, Boolean.class), false));
            } else if ((currentTxOption = CURRENT_TX_OPTION.get(this)) == null) {
                String m = "Not found cache current transaction option,you dont use jdbd-spi to control transaction.";
                mono = Mono.error(new JdbdException(m));
            } else if (currentTxOption instanceof LocalTxOption) {
                final LocalTxOption option = (LocalTxOption) currentTxOption;
                final Map<Option<?>, Object> map = MySQLCollections.hashMap(8);

                map.put(Option.IN_TRANSACTION, Boolean.TRUE);
                map.put(Option.ISOLATION, option.isolation); // MySQL don't support get current isolation level
                map.put(Option.READ_ONLY, readOnly);
                map.put(Option.WITH_CONSISTENT_SNAPSHOT, option.withConsistentSnapshot);

                mono = Mono.just(JdbdTransactionStatus.fromMap(map));
            } else if (currentTxOption instanceof XaTxOption) {
                final XaTxOption option = (XaTxOption) currentTxOption;
                final Map<Option<?>, Object> map = MySQLCollections.hashMap(11);

                map.put(Option.IN_TRANSACTION, Boolean.TRUE);
                map.put(Option.ISOLATION, option.isolation);
                map.put(Option.READ_ONLY, readOnly);
                map.put(Option.XID, option.xid);

                map.put(Option.XA_STATES, option.xaStates);
                map.put(Option.XA_FLAGS, option.flags);

                mono = Mono.just(JdbdTransactionStatus.fromMap(map));
            } else {
                // no bug,never here
                mono = Mono.error(new JdbdException(String.format("unknown current tx option %s", currentTxOption)));
            }
            return mono;
        }


    }// MySQLTaskAdjutant

    private static abstract class CurrentTxOption {

        final Isolation isolation;

        private CurrentTxOption(@Nullable Isolation isolation) {
            this.isolation = isolation;
        }

    }//CurrentTransactionOption

    private static final class LocalTxOption extends CurrentTxOption {

        private final Boolean withConsistentSnapshot;

        private LocalTxOption(@Nullable Isolation isolation, @Nullable Boolean withConsistentSnapshot) {
            super(isolation);
            this.withConsistentSnapshot = withConsistentSnapshot;
        }

    }//LocalTxOption

    private static final class XaTxOption extends CurrentTxOption {

        private final Xid xid;

        private final XaStates xaStates;

        private final int flags;

        private XaTxOption(@Nullable Isolation isolation, Xid xid, XaStates xaStates, int flags) {
            super(isolation);
            this.xid = xid;
            this.xaStates = xaStates;
            this.flags = flags;
        }

    } // XaTxOption


}