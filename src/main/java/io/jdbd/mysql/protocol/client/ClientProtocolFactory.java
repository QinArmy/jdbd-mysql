package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.SessionEnv;
import io.jdbd.mysql.env.MySQLHostInfo;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLProtocolFactory;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.util.*;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultItem;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.Option;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.SQLStates;
import io.netty.channel.ChannelOption;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * <p>
 * This class is protocol(connection) factory.
 * </p>
 *
 * @since 1.0
 */
public final class ClientProtocolFactory extends FixedEnv implements MySQLProtocolFactory {

    public static ClientProtocolFactory from(MySQLHostInfo host, boolean forPoolVendor) {
        if (host.isUnixDomainSocket()) {
            LOG.debug("use unix domain socket : {}", host.host());
        }
        return new ClientProtocolFactory(host, forPoolVendor);
    }


    private static final Logger LOG = LoggerFactory.getLogger(ClientProtocolFactory.class);


    final MySQLHostInfo mysqlHost;

    final int factoryTaskQueueSize;

    final boolean forPoolVendor;

    private final ConnectionProvider connectionProvider;

    private final LoopResources loopResources;

    private ClientProtocolFactory(final MySQLHostInfo host, boolean forPoolVendor) {
        super(host.properties());
        this.mysqlHost = host;
        this.forPoolVendor = forPoolVendor;

        final Environment env = this.env;
        this.factoryTaskQueueSize = env.getInRange(MySQLKey.FACTORY_TASK_QUEUE_SIZE, 3, 4096);
        this.connectionProvider = env.get(MySQLKey.CONNECTION_PROVIDER, ConnectionProvider::newConnection);
        this.loopResources = createEventLoopGroup(env);
    }


    @Override
    public String factoryName() {
        return this.env.getOrDefault(MySQLKey.FACTORY_NAME);
    }

    @Override
    public Mono<MySQLProtocol> createProtocol() {
        return this.newConnection() //1. create network connection
                .map(this::mapProtocolManager) // 2. map ClientProtocolManager
                .flatMap(ClientProtocolManager::authenticate) //3. authenticate
                .flatMap(ClientProtocolManager::initializing)//4. initializing
                .map(ClientProtocol::create);         //5. create ClientProtocol
    }


    @Override
    public <T> T valueOf(Option<T> option) {
        //TODO
        return null;
    }

    @Override
    public <T> Mono<T> close() {
        // io.jdbd.session.DatabaseSessionFactory is responsible for parallel.
        if (this.loopResources.isDisposed()) {
            return Mono.empty();
        }

        final Environment env = this.env;

        final Duration shutdownQuietPeriod, shutdownTimeout;
        shutdownQuietPeriod = Duration.ofMillis(env.getOrDefault(MySQLKey.SHUTDOWN_QUIET_PERIOD));
        shutdownTimeout = Duration.ofMillis(env.getOrDefault(MySQLKey.SHUTDOWN_TIMEOUT));

        return this.loopResources.disposeLater(shutdownQuietPeriod, shutdownTimeout)
                //.doOnTerminate(MySQLResultSetReader::deleteBigColumnTempDirectoryOnFactoryClose)  // TODO add ? conflict with CommandTask
                .then(Mono.empty());
    }

    @Override
    public boolean isClosed() {
        // io.jdbd.session.DatabaseSessionFactory is responsible for parallel.
        return this.loopResources.isDisposed();
    }

    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getName())
                .append("[ name : ")
                .append(this.factoryName())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }



    /*-------------------below private instance method -------------------*/


    /**
     * <p>
     * create new connection
     * </p>
     *
     * @see #createProtocol()
     */
    private Mono<? extends Connection> newConnection() {
        final MySQLHostInfo hostInfo = this.mysqlHost;
        final SocketAddress socketAddress;
        final boolean tcp;
        if (hostInfo.isUnixDomainSocket()) {
            socketAddress = new DomainSocketAddress(hostInfo.host());
            tcp = false;
        } else {
            socketAddress = InetSocketAddress.createUnresolved(hostInfo.host(), hostInfo.port());
            tcp = true;
        }
        final Environment env = this.env;

        return TcpClient.create(this.connectionProvider)
                .option(ChannelOption.SO_KEEPALIVE, tcp ? env.isOn(MySQLKey.TCP_KEEP_ALIVE) : null)
                .option(ChannelOption.TCP_NODELAY, tcp ? env.isOn(MySQLKey.TCP_NO_DELAY) : null)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, tcp ? env.getOrDefault(MySQLKey.CONNECT_TIMEOUT) : null)
                //.option(ChannelOption.AUTO_CLOSE, tcp ? false : null)
                .runOn(this.loopResources, true)
                .remoteAddress(() -> socketAddress)
                .connect()
                .onErrorMap(ClientProtocolFactory::mapConnectionError);
    }

    /**
     * @see #newConnection()
     */
    private ClientProtocolManager mapProtocolManager(Connection connection) {
        return new ClientProtocolManager(MySQLTaskExecutor.create(this, connection), this);
    }


    /*-------------------below static method -------------------*/

    private static JdbdException mapConnectionError(final Throwable cause) {
        final JdbdException error;
        if (cause instanceof JdbdException) {
            error = (JdbdException) cause;
        } else if (cause instanceof ClosedChannelException) {
            error = new JdbdException("connect failure,possibly server too busy", cause);
        } else if (cause instanceof ConnectException) {
            error = new JdbdException(String.format("connect failure, %s", cause.getMessage()), cause);
        } else {
            error = new JdbdException("connect error, unknown error", cause);
        }
        return error;
    }


    private static LoopResources createEventLoopGroup(final Environment env) {
        final int workerCount;
        workerCount = env.getInRange(MySQLKey.FACTORY_WORKER_COUNT, 2, Integer.MAX_VALUE);
        int selectCount;
        selectCount = env.getOrDefault(MySQLKey.FACTORY_SELECT_COUNT_COUNT);
        if (selectCount < 0) {
            selectCount = workerCount;
        }
        return LoopResources.create("jdbd-mysql", selectCount, workerCount, true);
    }


    private static final class ClientProtocolManager implements ProtocolManager {

        private static final String CHARACTER_SET_CLIENT = "character_set_client";
        private static final String CHARACTER_SET_RESULTS = "character_set_results";
        private static final String COLLATION_CONNECTION = "collation_connection";
        private static final String RESULTSET_METADATA = "resultset_metadata";

        private static final String TIME_ZONE = "time_zone";


        private static final List<String> KEY_VARIABLES = MySQLArrays.unmodifiableListOf(
                "sql_mode",
                "time_zone",
                "transaction_isolation",
                "transaction_read_only",
                "autocommit"
        );

        private final MySQLTaskExecutor executor;

        private final TaskAdjutant adjutant;

        private final ClientProtocolFactory factory;

        private ClientProtocolManager(MySQLTaskExecutor executor, ClientProtocolFactory factory) {
            this.executor = executor;
            this.adjutant = executor.taskAdjutant();
            this.factory = factory;
        }


        @Override
        public TaskAdjutant adjutant() {
            return this.executor.taskAdjutant();
        }

        @Override
        public Mono<Void> reset() {
            LOG.debug("reset session");
            return ComResetTask.reset(this.adjutant)
                    .then(Mono.defer(this::initializeCustomCharset))
                    .then(Mono.defer(this::resetSessionEnvironment))
                    .then();
        }


        private Mono<ClientProtocolManager> authenticate() {
            LOG.debug("authenticate");
            return MySQLConnectionTask.authenticate(this.adjutant)
                    .doOnSuccess(this.executor::setAuthenticateResult)
                    .thenReturn(this);
        }


        /**
         * Just for
         * <ul>
         *     <li>{@link ClientProtocolFactory#createProtocol() }</li>
         * </ul>
         *
         * @see ClientProtocolFactory#createProtocol()
         */
        private Mono<ClientProtocolManager> initializing() {
            LOG.debug("initializing connection");
            return this.initializeCustomCharset()
                    .then(Mono.defer(this::resetSessionEnvironment))
                    .onErrorResume(this::onInitializingError)
                    .thenReturn(this);
        }

        /**
         * @see #initializing()
         */
        private Mono<ClientProtocolManager> onInitializingError(Throwable error) {
            return QuitTask.quit(this.adjutant)
                    .then(Mono.error(error));
        }


        /**
         * @see #initializing()
         */
        private Mono<ProtocolManager> resetSessionEnvironment() {
            LOG.debug("reset session environment");

            final StringBuilder builder = new StringBuilder(480);

            final Charset clientCharset, resultCharset;
            final ZoneOffset connZone;
            try {

                keyVariablesDefaultSql(builder);
                buildSetVariableCommand(builder);
                connCollation(builder);
                clientCharset = clientMyCharset(builder);

                resultCharset = resultsCharset(builder);
                connZone = connTimeZone(builder); // after keyVariablesDefaultSql
            } catch (Throwable e) {
                return Mono.error(MySQLExceptions.wrap(e));
            }


            final long utcEpochSecond;
            utcEpochSecond = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond();
            if (builder.length() > 0) {
                builder.append(Constants.SPACE_SEMICOLON_SPACE);
            }
            builder.append("SELECT DATE_FORMAT(FROM_UNIXTIME(")
                    .append(utcEpochSecond)
                    .append("),'%Y-%m-%d %T') AS databaseNow,@@SESSION.sql_mode AS sqlMode,@@GLOBAL.local_infile AS localInfile")
                    .append(",@@GLOBAL.max_allowed_packet AS globalMaxPacket,@@SESSION.max_allowed_packet AS sessionMaxPacket");

            return Flux.from(ComQueryTask.staticMultiStmt(Stmts.multiStmt(builder.toString()), this.adjutant))
                    .filter(ResultItem::isRowItem)
                    .last()
                    .map(ResultRow.class::cast)
                    .map(row -> new DefaultSessionEnv(clientCharset, resultCharset, connZone, row, utcEpochSecond, this.factory.env))
                    .doOnSuccess(this.executor::resetTaskAdjutant)

                    .switchIfEmpty(Mono.defer(this::resetFailure))
                    .thenReturn(this);
        }

        /**
         * @see #initializing()
         */
        private Mono<Void> initializeCustomCharset() {
            if (this.factory.env.isOff(MySQLKey.DETECT_CUSTOM_COLLATIONS) || this.factory.customCharsetMap.size() == 0) {
                return Mono.empty();
            }
            return ComQueryTask.query(Stmts.stmt("SHOW CHARACTER SET"), this::mapMyCharset, ResultStates.IGNORE_STATES, this.adjutant)// SHOW CHARACTER SET result
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collectMap(MyCharset::charsetName, MyCharset::self, MySQLCollections::hashMap)
                    .map(MySQLCollections::unmodifiableMap)
                    .flatMap(this::queryAndHandleCollation); // query SHOW COLLATION result
        }


        /**
         * @see #initializeCustomCharset()
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-mysql.html">Character Sets and Collations in MySQL</a>
         */
        private Optional<MyCharset> mapMyCharset(final CurrentRow row) {
            final String name;
            name = row.getNonNull(0, String.class); // Charset
            final Charset charset;
            if (Charsets.NAME_TO_CHARSET.containsKey(name)
                    || (charset = this.factory.customCharsetMap.get(name)) == null) {
                return Optional.empty();
            }
            final MyCharset myCharset;
            myCharset = new MyCharset(name, row.getNonNull(3, Integer.class), 0, charset.name()); // Maxlen
            return Optional.of(myCharset);
        }


        /**
         * @see #initializeCustomCharset()
         */
        private Mono<Void> queryAndHandleCollation(final Map<String, MyCharset> charsetMap) {

            if (charsetMap.size() == 0) {
                return Mono.empty();
            }
            final ParamStmt stmt;
            stmt = collationStmt(charsetMap);
            return ComQueryTask.paramQuery(stmt, row -> mapCollation(row, charsetMap), ResultStates.IGNORE_STATES, this.adjutant)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collectMap(Collation::index, Collation::self, MySQLCollections::hashMap)
                    .map(MySQLCollections::unmodifiableMap)
                    .flatMap(customCollationMap -> this.executor.setCustomCollation(charsetMap, customCollationMap))
                    .then();
        }

        /**
         * @see #queryAndHandleCollation(Map)
         */
        private Optional<Collation> mapCollation(final CurrentRow row, final Map<String, MyCharset> charsetMap) {
            final String charsetName;
            charsetName = row.getNonNull("Charset", String.class);
            final MyCharset charset;
            charset = charsetMap.get(charsetName);
            if (charset == null) {
                return Optional.empty();
            }
            final Collation collation;
            collation = new Collation(row.getNonNull("Id", Integer.class), charsetName, 0, charset);
            return Optional.of(collation);
        }

        /**
         * @see #queryAndHandleCollation(Map)
         */
        private ParamStmt collationStmt(final Map<String, MyCharset> charsetMap) {
            final int charsetCount;
            charsetCount = charsetMap.size();

            final StringBuilder builder = new StringBuilder(128);
            builder.append("SHOW COLLATION WHERE Charset IN ( ");
            int index = 0;
            final List<ParamValue> paramList = MySQLCollections.arrayList(charsetCount);
            for (String name : charsetMap.keySet()) {
                if (index > 0) {
                    builder.append(" ,");
                }
                builder.append(" ?");
                paramList.add(JdbdValues.paramValue(index, MySQLType.VARCHAR, name));
                index++;
            }
            builder.append(" )");
            return Stmts.paramStmt(builder.toString(), paramList);
        }


        /**
         * @see #resetSessionEnvironment()
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html#time-zone-variables">Time Zone Variables</a>
         */
        @Nullable
        private ZoneOffset connTimeZone(final StringBuilder builder) {
            final String zoneStr;
            zoneStr = this.factory.env.get(MySQLKey.CONNECTION_TIME_ZONE);
            if (zoneStr == null || Constants.SERVER.equals(zoneStr)) {
                return null;
            }

            final ZoneOffset zone;
            if (Constants.LOCAL.equals(zoneStr)) {
                zone = MySQLTimes.systemZoneOffset();
            } else {
                zone = ZoneOffset.of(zoneStr);
            }

            if (this.factory.env.getOrDefault(MySQLKey.FORCE_CONNECTION_TIME_ZONE_TO_SESSION)) {
                if (builder.length() > 0) {
                    builder.append(Constants.SPACE_SEMICOLON_SPACE);
                }
                builder.append("SET @@SESSION.time_zone = '")
                        .append(MySQLTimes.ZONE_FORMATTER.format(zone))
                        .append(Constants.QUOTE);

            }
            return zone;
        }

        /**
         * @see #resetSessionEnvironment()
         */
        private void keyVariablesDefaultSql(final StringBuilder builder) {
            if (builder.length() > 0) {
                builder.append(Constants.SPACE_SEMICOLON_SPACE);
            }
            builder.append("SET");

            final List<String> list = KEY_VARIABLES;
            final int size = list.size();

            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    builder.append(Constants.SPACE_COMMA_SPACE);
                }

                builder.append(" @@SESSION.")
                        .append(list.get(i))
                        .append(" = DEFAULT");

            }

        }

        /**
         * @see #resetSessionEnvironment()
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html">Connection Character Sets and Collations</a>
         */
        @Nullable
        private Charset resultsCharset(final StringBuilder builder) throws JdbdException {
            final String charsetString = this.factory.env.get(MySQLKey.CHARACTER_SET_RESULTS);

            if (builder.length() > 0) {
                builder.append(Constants.SPACE_SEMICOLON_SPACE);
            }

            final Charset charsetResults;
            builder.append("SET character_set_results = ");
            if (charsetString == null || charsetString.equalsIgnoreCase(Constants.NULL)) {
                builder.append(Constants.NULL);
                charsetResults = null;
            } else if (charsetString.equalsIgnoreCase("binary")) {
                builder.append("binary");
                charsetResults = null; // must be null
            } else {
                MyCharset myCharset;
                myCharset = Charsets.NAME_TO_CHARSET.get(charsetString.toLowerCase());
                if (myCharset == null) {
                    myCharset = this.adjutant.nameCharsetMap().get(charsetString.toLowerCase());
                }
                if (myCharset == null) {
                    String message = String.format("No found MySQL charset[%s] fro Property[%s]",
                            charsetString, MySQLKey.CHARACTER_SET_RESULTS);
                    throw new JdbdException(message, SQLStates.CONNECTION_EXCEPTION, 0);
                }

                charsetResults = Charset.forName(myCharset.javaEncodingsUcList.get(0));
                builder.append(Constants.QUOTE)
                        .append(myCharset.name)
                        .append(Constants.QUOTE);
            }

            return charsetResults;
        }

        /**
         * @see #resetSessionEnvironment()
         */
        private void connCollation(final StringBuilder builder) throws JdbdException {
            final String collationStr;
            collationStr = this.factory.env.get(MySQLKey.CONNECTION_COLLATION);

            Collation collation;
            if (!MySQLStrings.hasText(collationStr)) {
                collation = null;
            } else if ((collation = Charsets.getCollationByName(collationStr)) == null) {
                collation = this.adjutant.nameCollationMap().get(collationStr.toLowerCase());
                if (collation == null) {
                    String message = String.format("No found MySQL Collation[%s] fro Property[%s]",
                            collationStr, MySQLKey.CONNECTION_COLLATION);
                    throw new JdbdException(message, SQLStates.CONNECTION_EXCEPTION, 0);
                }
            }

            if (collation != null) {
                final Charset charset;
                charset = Charset.forName(collation.myCharset.javaEncodingsUcList.get(0));
                if (!Charsets.isSupportCharsetClient(charset)) {
                    collation = null;
                }
            }

            final String mysqlCharsetName;
            if (collation == null) {
                mysqlCharsetName = Charsets.utf8mb4;
            } else {
                mysqlCharsetName = collation.myCharset.name;
            }

            if (builder.length() > 0) {
                builder.append(Constants.SPACE_SEMICOLON_SPACE);
            }

            builder.append("SET character_set_connection = '")
                    .append(mysqlCharsetName)
                    .append(Constants.QUOTE);

            if (collation != null) {
                builder.append(" COLLATE '")
                        .append(collation.name)
                        .append(Constants.QUOTE);
            }

        }

        /**
         * @see #resetSessionEnvironment()
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html">Connection Character Sets and Collations</a>
         */
        private Charset clientMyCharset(final StringBuilder builder) {
            Charset charset;
            charset = this.factory.env.getOrDefault(MySQLKey.CHARACTER_ENCODING);

            MyCharset myCharset = null;
            if (charset != StandardCharsets.UTF_8 && Charsets.isSupportCharsetClient(charset)) {
                final MySQLServerVersion version = this.adjutant.handshake10().serverVersion;
                myCharset = Charsets.getMysqlCharsetForJavaEncoding(charset.name(), version);
            }
            if (myCharset == null) {
                charset = StandardCharsets.UTF_8;
                myCharset = Charsets.NAME_TO_CHARSET.get(Charsets.utf8mb4);
            }

            if (builder.length() > 0) {
                builder.append(Constants.SPACE_SEMICOLON_SPACE);
            }

            builder.append("SET character_set_client = '")
                    .append(myCharset.name)
                    .append(Constants.QUOTE);

            return charset;
        }


        /**
         * @see #resetSessionEnvironment()
         */
        @Nullable
        private void buildSetVariableCommand(final StringBuilder builder) throws JdbdException {

            final String variables;
            variables = this.factory.env.get(MySQLKey.SESSION_VARIABLES);

            if (variables == null) {
                return;
            }
            final Map<String, String> map;
            map = MySQLStrings.spitAsMap(variables, ",", "=", true);

            if (builder.length() > 0) {
                builder.append(Constants.SPACE_SEMICOLON_SPACE);
            }

            builder.append("SET ");
            int index = 0;
            String lower, name, value;
            for (Map.Entry<String, String> e : map.entrySet()) {
                name = e.getKey();

                lower = name.toLowerCase(Locale.ROOT);
                if (lower.contains(CHARACTER_SET_RESULTS)
                        || lower.contains(CHARACTER_SET_CLIENT)
                        || lower.contains(COLLATION_CONNECTION)
                        || lower.contains(RESULTSET_METADATA)
                        || lower.contains(TIME_ZONE)) {
                    throw createSetVariableException();
                }

                value = e.getValue();
                if (!MySQLStrings.isSimpleIdentifier(name) || value.contains("'") || value.contains("\\")) {
                    throw MySQLExceptions.valueErrorOfKey(MySQLKey.SESSION_VARIABLES.name);
                }

                if (index > 0) {
                    builder.append(" ,");
                }
                builder.append(" @@SESSION.")
                        .append(name)
                        .append(" = '")
                        .append(value)
                        .append(Constants.QUOTE);

                index++;

            }
        }

        /**
         * @see #reset()
         */
        private <T> Mono<T> resetFailure() {
            // not bug ,never here.
            return Mono.error(new JdbdException("reset failure,no any result"));
        }

        /**
         * @see #buildSetVariableCommand(StringBuilder)
         */
        private static JdbdException createSetVariableException() {
            String message = String.format("Below three session variables[%s,%s,%s,%s] must specified by below three properties[%s,%s,%s].",
                    CHARACTER_SET_CLIENT,
                    CHARACTER_SET_RESULTS,
                    COLLATION_CONNECTION,
                    RESULTSET_METADATA,
                    MySQLKey.CHARACTER_ENCODING,
                    MySQLKey.CHARACTER_SET_RESULTS,
                    MySQLKey.CONNECTION_COLLATION
            );
            return new JdbdException(message);
        }


    } // ClientProtocolManager


    private static ZoneOffset parseServerZone(final ResultRow row, long utcEpochSecond) {
        final LocalDateTime dateTime;
        dateTime = LocalDateTime.parse(row.getNonNull("databaseNow", String.class), MySQLTimes.DATETIME_FORMATTER_6);

        final int totalSeconds;
        totalSeconds = (int) (OffsetDateTime.of(dateTime, ZoneOffset.UTC).toEpochSecond() - utcEpochSecond);
        return ZoneOffset.ofTotalSeconds(totalSeconds);
    }


    private static final class DefaultSessionEnv implements SessionEnv {

        private final Charset clientCharset;

        private final Charset resultsCharset;

        private final ZoneOffset serverZone;

        private final ZoneOffset connZone;

        private final Set<String> sqlModeSet;

        private final boolean localInfile;

        private final int globalMaxPacket;

        private final int sessionMaxPacket;


        private DefaultSessionEnv(Charset clientCharset, @Nullable Charset resultsCharset, @Nullable ZoneOffset connZone,
                                  ResultRow row, long utcEpochSecond, Environment env) {
            Objects.requireNonNull(clientCharset);
            this.clientCharset = clientCharset;
            this.resultsCharset = StandardCharsets.ISO_8859_1.equals(resultsCharset) ? null : resultsCharset;

            this.serverZone = parseServerZone(row, utcEpochSecond);

            if ("SERVER".equals(env.get(MySQLKey.CONNECTION_TIME_ZONE))) {
                this.connZone = this.serverZone;
            } else {
                this.connZone = connZone;
            }
            this.sqlModeSet = MySQLStrings.spitAsSet(row.get("sqlMode", String.class), ",", true);

            this.localInfile = row.getNonNull("localInfile", Boolean.class);
            this.globalMaxPacket = row.getNonNull("globalMaxPacket", Integer.class);
            this.sessionMaxPacket = row.getNonNull("sessionMaxPacket", Integer.class);
        }


        @Override
        public boolean containSqlMode(final SQLMode sqlMode) {
            return this.sqlModeSet.contains(sqlMode.name());
        }

        @Override
        public Charset charsetClient() {
            return this.clientCharset;
        }

        @Override
        public Charset charsetResults() {
            return this.resultsCharset;
        }

        @Override
        public ZoneOffset connZone() {
            return this.connZone;
        }

        @Override
        public ZoneOffset serverZone() {
            return this.serverZone;
        }

        @Override
        public boolean isSupportLocalInfile() {
            return this.localInfile;
        }

        @Override
        public int globalMaxAllowedPacket() {
            return this.globalMaxPacket;
        }

        @Override
        public int sessionMaxAllowedPacket() {
            return this.sessionMaxPacket;
        }

        @Override
        public String toString() {
            return MySQLStrings.builder()
                    .append(getClass().getSimpleName())
                    .append("[ sqlModeSet : ")
                    .append(this.sqlModeSet)
                    .append(" , clientCharset : ")
                    .append(this.clientCharset)
                    .append(" , resultsCharset : ")
                    .append(this.resultsCharset)
                    .append(" , connZone : ")
                    .append(this.connZone)
                    .append(" , serverZone : ")
                    .append(this.serverZone)
                    .append(" , localInfile : ")
                    .append(this.localInfile)
                    .append(" , globalMaxAllowedPacket : ")
                    .append(this.globalMaxPacket)
                    .append(" , sessionMaxAllowedPacket : ")
                    .append(this.sessionMaxPacket)
                    .append(" ]")
                    .toString();
        }


    }//DefaultSessionEnv


}
