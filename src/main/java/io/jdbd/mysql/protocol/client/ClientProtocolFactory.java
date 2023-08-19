package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.SessionEnv;
import io.jdbd.mysql.env.MySQLHost;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.env.Protocol;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLProtocolFactory;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.util.*;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmts;
import io.jdbd.vendor.util.SQLStates;
import io.netty.channel.EventLoopGroup;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpResources;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Consumer;

public final class ClientProtocolFactory extends FixedEnv implements MySQLProtocolFactory {

    public static ClientProtocolFactory from(MySQLHost host) {
        if (host.protocol() != Protocol.SINGLE_CONNECTION) {
            throw new IllegalArgumentException();
        }
        return new ClientProtocolFactory(host);
    }


    final MySQLHost host;

    final int factoryTaskQueueSize;

    final TcpClient tcpClient;

    private ClientProtocolFactory(final MySQLHost host) {
        super(host.properties());
        this.host = host;

        this.factoryTaskQueueSize = this.env.getInRange(MySQLKey.FACTORY_TASK_QUEUE_SIZE, 3, 4096);

        this.tcpClient = TcpClient.create(this.env.get(MySQLKey.SOCKET_FACTORY, TcpResources::get))
                .runOn(createEventLoopGroup(this.env))
                .host(host.host())
                .port(host.port());
    }

    @Override
    public String factoryName() {
        return this.env.getOrDefault(MySQLKey.FACTORY_NAME);
    }

    @Override
    public Mono<MySQLProtocol> createProtocol() {
        return MySQLTaskExecutor.create(this)//1. create tcp connection
                .map(executor -> new ClientProtocolManager(executor, this))//2. create  SessionManagerImpl
                .flatMap(ClientProtocolManager::authenticate) //3. authenticate
                .flatMap(ClientProtocolManager::initializing)//4. initializing
                .map(ClientProtocol::create);         //5. create ClientProtocol
    }


    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getName())
                .append("[ name : ")
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }

    private static EventLoopGroup createEventLoopGroup(final Environment env) {
        return LoopResources.create("jdbd-mysql", env.getOrDefault(MySQLKey.FACTORY_WORKER_COUNT), true)
                .onClient(true);
    }


    private static final class ClientProtocolManager implements ProtocolManager {

        private static final String CHARACTER_SET_CLIENT = "character_set_client";
        private static final String CHARACTER_SET_RESULTS = "character_set_results";
        private static final String COLLATION_CONNECTION = "collation_connection";
        private static final String RESULTSET_METADATA = "resultset_metadata";

        private static final String TIME_ZONE = "time_zone";


        private static final List<String> KEY_VARIABLES = MySQLArrays.asUnmodifiableList(
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
            return ComResetTask.reset(this.adjutant)
                    .then(Mono.defer(this::resetSessionEnvironment))
                    .then();
        }


        @Override
        public Mono<Void> reConnect() {
            return this.executor.reConnect()
                    .then(Mono.defer(this::authenticate))
                    .then(Mono.defer(this::initializing))
                    .then();
        }


        private Mono<ClientProtocolManager> authenticate() {
            return MySQLConnectionTask.authenticate(this.executor.taskAdjutant())
                    .doOnSuccess(this.executor::setAuthenticateResult)
                    .thenReturn(this);
        }


        private Mono<ClientProtocolManager> initializing() {
            return this.initializeCustomCharset()
                    .then(Mono.defer(this::resetSessionEnvironment))
                    .thenReturn(this);
        }


        /**
         * @see #initializing()
         */
        private Mono<ProtocolManager> resetSessionEnvironment() {
            final List<String> sqlGroup = MySQLCollections.arrayList(6);

            final Charset clientCharset, resultCharset;
            final ZoneOffset connZone;
            try {
                final Consumer<String> sqlConsumer = sqlGroup::add;

                buildSetVariableCommand(sqlConsumer);
                sqlGroup.add(connCollation());
                clientCharset = clientMyCharset(sqlConsumer);
                resultCharset = resultsCharset(sqlConsumer);

                sqlGroup.add(keyVariablesDefaultSql());
                connZone = connTimeZone(sqlConsumer); // after keyVariablesDefaultSql
            } catch (Throwable e) {
                return Mono.error(MySQLExceptions.wrap(e));
            }


            final long utcEpochSecond;
            utcEpochSecond = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond();
            final String keyVariablesQuerySql;
            keyVariablesQuerySql = MySQLStrings.builder()
                    .append("SELECT DATE_FORMAT(FROM_UNIXTIME(")
                    .append(utcEpochSecond)
                    .append("),'%Y-%m-%d %T') AS databaseNow , @@SESSION.sql_mode AS sqlMode , @@SESSION.local_infile localInfile")
                    .toString();

            sqlGroup.add(keyVariablesQuerySql); // 5.
            final int sqlSize = sqlGroup.size();

            return Flux.from(ComQueryTask.batchAsFlux(Stmts.batch(sqlGroup), this.adjutant))
                    .filter(r -> r instanceof ResultRow && r.getResultNo() == sqlSize)
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
            if (!this.factory.env.getOrDefault(MySQLKey.DETECT_CUSTOM_COLLATIONS)
                    || this.factory.customCharsetMap.size() == 0) {
                return Mono.empty();
            }
            return ComQueryTask.query(Stmts.stmt("SHOW CHARACTER SET"), this::mapMyCharset, this.adjutant)// SHOW CHARACTER SET result
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collectMap(MyCharset::charsetName, MyCharset::self, MySQLCollections::hashMap)
                    .map(MySQLCollections::unmodifiableMap)
                    .flatMap(this::queryAndHandleCollation); // query SHOW COLLATION result
        }


        /**
         * @see #initializeCustomCharset()
         */
        private Optional<MyCharset> mapMyCharset(final CurrentRow row) {
            final String name;
            name = row.getNonNull("Charset", String.class);
            final Charset charset;
            if (Charsets.NAME_TO_CHARSET.containsKey(name)
                    || (charset = this.factory.customCharsetMap.get(name)) == null) {
                return Optional.empty();
            }
            final MyCharset myCharset;
            myCharset = new MyCharset(name, row.getNonNull("Maxlen", Integer.class), 0, charset.name());
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
            return ComQueryTask.paramQuery(stmt, row -> mapCollation(row, charsetMap), this.adjutant)
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
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html#time-zone-variables">Time Zone Variables</a>
         */
        @Nullable
        private ZoneOffset connTimeZone(final Consumer<String> sqlConsumer) {
            final String zoneStr;
            zoneStr = this.factory.env.get(MySQLKey.CONNECTION_TIME_ZONE);
            if (zoneStr == null || "SERVER".equals(zoneStr)) {
                return null;
            }

            final ZoneOffset zone;
            if (zoneStr.equals("LOCAL")) {
                zone = MySQLTimes.systemZoneOffset();
            } else {
                zone = ZoneOffset.of(zoneStr);
            }

            if (this.factory.env.getOrDefault(MySQLKey.FORCE_CONNECTION_TIME_ZONE_TO_SESSION)) {
                final String command;
                command = MySQLStrings.builder()
                        .append("SET @@SESSION.time_zone = '")
                        .append(MySQLTimes.ZONE_FORMATTER.format(zone))
                        .append(Constants.QUOTE)
                        .toString();

                sqlConsumer.accept(command);
            }
            return zone;
        }

        private String keyVariablesDefaultSql() {
            final StringBuilder builder = new StringBuilder(50);
            builder.append("SET");

            final List<String> list = KEY_VARIABLES;
            final int size = list.size();

            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    builder.append(" ,");
                }

                builder.append(" @@SESSION.")
                        .append(list.get(i))
                        .append(" = DEFAULT");

            }
            return builder.toString();
        }


        private Charset resultsCharset(final Consumer<String> sqlConsumer) throws JdbdException {
            final String charsetString = this.factory.env.get(MySQLKey.CHARACTER_SET_RESULTS);

            final StringBuilder builder = new StringBuilder(40);
            final Charset charsetResults;
            builder.append("SET character_set_results = ");
            if (charsetString == null || charsetString.equalsIgnoreCase(Constants.NULL)) {
                builder.append(Constants.NULL);
                charsetResults = StandardCharsets.ISO_8859_1;
            } else if (charsetString.equalsIgnoreCase("binary")) {
                builder.append("binary");
                charsetResults = StandardCharsets.ISO_8859_1;
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

            sqlConsumer.accept(builder.toString());
            return charsetResults;
        }

        /**
         * @see #reset()
         */
        private String connCollation() throws JdbdException {
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

            final StringBuilder builder = new StringBuilder(40);
            builder.append("SET character_set_connection = '")
                    .append(mysqlCharsetName)
                    .append(Constants.QUOTE);

            if (collation != null) {
                builder.append(" COLLATE '")
                        .append(collation.name)
                        .append(Constants.QUOTE);
            }
            return builder.toString();
        }

        /**
         * @see #reset()
         */
        private Charset clientMyCharset(final Consumer<String> sqlConsumer) {
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

            final StringBuilder builder;
            builder = MySQLStrings.builder()
                    .append("SET character_set_client '")
                    .append(myCharset.name)
                    .append(Constants.QUOTE);

            sqlConsumer.accept(builder.toString());

            return charset;
        }


        @Nullable
        private void buildSetVariableCommand(final Consumer<String> sqlConsumer) throws JdbdException {

            final String variables;
            variables = this.factory.env.get(MySQLKey.SESSION_VARIABLES);

            if (variables == null) {
                return;
            }
            final Map<String, String> map;
            map = MySQLStrings.spitAsMap(variables, ",", "=", true);

            final StringBuilder builder = new StringBuilder(40);
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
                        .append('\'');

                index++;

            }

            sqlConsumer.accept(builder.toString());
        }

        /**
         * @see #reset()
         */
        private <T> Mono<T> resetFailure() {
            // not bug ,never here.
            return Mono.error(new JdbdException("reset failure,no any result"));
        }

        /**
         * @see #buildSetVariableCommand(Consumer)
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
                    .append(" ]")
                    .toString();
        }


    }//DefaultSessionEnv


}
