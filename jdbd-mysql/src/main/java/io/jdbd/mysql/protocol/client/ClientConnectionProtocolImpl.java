package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.*;
import io.jdbd.mysql.protocol.authentication.*;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.*;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import org.qinarmy.util.Pair;
import org.qinarmy.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.FutureMono;
import reactor.netty.NettyPipeline;
import reactor.netty.tcp.TcpClient;
import reactor.util.annotation.Nullable;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class ClientConnectionProtocolImpl extends AbstractClientProtocol implements ClientConnectionProtocol, ProtocolAssistant {

    private static final Logger LOG = LoggerFactory.getLogger(ClientConnectionProtocolImpl.class);


    public static Mono<ClientConnectionProtocol> from(MySQLUrl mySQLUrl) {
        if (mySQLUrl.getProtocol() != MySQLUrl.Protocol.SINGLE_CONNECTION) {
            throw new IllegalArgumentException(
                    String.format("mySQLUrl protocol isn't %s", MySQLUrl.Protocol.SINGLE_CONNECTION));
        }
        HostInfo hostInfo = mySQLUrl.getHosts().get(0);
        return TcpClient.create()
                .host(hostInfo.getHost())
                .port(hostInfo.getPort())
                .connect()
                // create ClientCommandProtocolImpl instance
                .map(connection -> new ClientConnectionProtocolImpl(mySQLUrl, connection))
                ;
    }


    private static final String NONE = "none";

    static final String CHARACTER_SET_CLIENT = "character_set_client";
    static final String CHARACTER_SET_RESULTS = "character_set_results";
    static final String COLLATION_CONNECTION = "collation_connection";


    /**
     * below {@code xxx_PHASE } representing connection phase.
     *
     * @see #connectionPhase
     */
    private static final int HANDSHAKE_PHASE = 0;
    private static final int SSL_PHASE = 1;
    private static final int AUTHENTICATION_PHASE = 2;
    private static final int CONFIGURE_SESSION_PHASE = 3;

    private static final int INITIALIZING_PHASE = 4;
    private static final int COMMAND_PHASE = 5;


    private final AtomicReference<HandshakeV10Packet> handshakeV10Packet = new AtomicReference<>(null);

    private final Properties properties;

    /**
     * connection phase client charset,will send to server by {@code Protocol::HandshakeResponse41}.
     */
    private final Charset handshakeCharset;

    private final AtomicReference<Integer> negotiatedCapability = new AtomicReference<>(null);

    private final AtomicInteger handshakeSequenceId = new AtomicInteger(-1);

    private final AtomicReference<ServerStatus> oldServerStatus = new AtomicReference<>(null);

    private final AtomicReference<ServerStatus> serverStatus = new AtomicReference<>(null);

    private final AtomicInteger authCounter = new AtomicInteger(0);

    private final AtomicInteger connectionPhase = new AtomicInteger(HANDSHAKE_PHASE);

    private final AtomicInteger handshakeCollationIndex = new AtomicInteger(0);

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html"> character_set_client system variable</a>
     * @see PropertyKey#characterEncoding
     */
    private final AtomicReference<Charset> charsetClient = new AtomicReference<>(null);

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html"> character_set_results system variable</a>
     * @see PropertyKey#characterSetResults
     */
    private final AtomicReference<Charset> charsetResults = new AtomicReference<>(null);

    private final AtomicReference<Map<Integer, CharsetMapping.CustomCollation>> customCollationMap = new AtomicReference<>(null);

    private final AtomicReference<Set<String>> customCharsetNameSet = new AtomicReference<>(null);

    private final AtomicReference<ZoneOffset> zoneOffsetDatabase = new AtomicReference<>(null);

    private final AtomicReference<ZoneOffset> zoneOffsetClient = new AtomicReference<>(null);


    private ClientConnectionProtocolImpl(MySQLUrl mySQLUrl, Connection connection) {
        super(connection, mySQLUrl, MySQLCumulateSubscriber.from(connection));

        this.properties = mySQLUrl.getHosts().get(0).getProperties();
        this.handshakeCharset = this.properties.getProperty(PropertyKey.characterEncoding
                , Charset.class, StandardCharsets.UTF_8);
    }


    @Override
    public Mono<Void> authenticateAndInitializing() {
        return receiveHandshake()
                .then(Mono.defer(this::sslNegotiate))
                .then(Mono.defer(this::authenticate))
                .then(Mono.defer(this::configureSession))
                .then(Mono.defer(this::initialize))
                ;
    }

    /*################################## blow ProtocolAssistant method ##################################*/

    @Override
    public Charset getHandshakeCharset() {
        return this.handshakeCharset;
    }

    @Override
    public Charset getPasswordCharset() {
        String pwdCharset = this.properties.getProperty(PropertyKey.passwordCharacterEncoding);
        return pwdCharset == null ? this.handshakeCharset : Charset.forName(pwdCharset);
    }

    @Override
    public HostInfo getMainHostInfo() {
        return this.mySQLUrl.getHosts().get(0);
    }

    @Override
    public boolean isUseSsl() {
        return (obtainNegotiatedCapability() & CLIENT_SSL) != 0;
    }


    @Override
    public ByteBuf createPayloadBuffer(int initialPayloadCapacity) {
        return this.connection.outbound().alloc().buffer(initialPayloadCapacity);
    }

    @Override
    public ByteBuf createOneSizePayload(int payloadByte) {
        return this.connection.outbound().alloc().buffer(1)
                .writeByte(payloadByte)
                .asReadOnly();
    }

    @Override
    public ByteBuf createEmptyPayload() {
        return this.connection.outbound().alloc().buffer(0).asReadOnly();
    }

    @Override
    public ServerVersion getServerVersion() {
        return obtainHandshakeV10Packet().getServerVersion();
    }

    /*################################## blow ResultRowAdjutant method ##################################*/

    @Override
    public ZoneOffset obtainClientZoneOffset() {
        return ZonedDateTime.now(ZoneId.systemDefault()).getOffset();
    }


    /*################################## blow package method ##################################*/


    /**
     * <p>
     * must invoke firstly .
     * </p>
     * <p>
     * Receive HandshakeV10 packet send by MySQL server.
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html">Protocol::HandshakeV10</a>
     */
    Mono<MySQLPacket> receiveHandshake() {
        final int phase = this.connectionPhase.get();
        if (phase != HANDSHAKE_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        return receivePayload()
                .flatMap(this::readHandshakeV10Packet)
                .flatMap(this::handleHandshakeV10Packet)
                .cast(MySQLPacket.class)
                ;
    }

    /**
     * negotiate ssl for connection than hold by this instance.
     * <p>
     * must invoke after {@link #receiveHandshake()} and before {@link #authenticateAndInitializing()}
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html#sect_protocol_connection_phase_initial_handshake_ssl_handshake">Protocol::SSL Handshake</a>
     */
    Mono<PropertyDefinitions.SslMode> sslNegotiate() {
        final int phase = this.connectionPhase.get();
        if (phase != SSL_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        if ((obtainNegotiatedCapability() & CLIENT_SSL) == 0) {
            return doAfterSendSslRequestEnd();
        }
        return sendHandshakePacket(createSslRequest41Packet())
                .then(Mono.defer(this::performSslHandshake))
                .then(Mono.defer(this::doAfterSendSslRequestEnd))
                ;
    }

    /**
     * <p>
     * must invoke after {@link #sslNegotiate()}.
     * </p>
     * <p>
     * do below:
     *     <ol>
     *     <li>send HandshakeResponse41 packet.</li>
     *     <li>handle more authentication exchange.</li>
     *     </ol>
     * </p>
     */
    Mono<Void> authenticate() {
        final int phase = this.connectionPhase.get();
        if (phase != AUTHENTICATION_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        final Triple<AuthenticationPlugin, Boolean, Map<String, AuthenticationPlugin>> triple;
        //1. obtain plugin
        triple = obtainAuthenticationPlugin();

        final AuthenticationPlugin plugin = triple.getFirst();
        //2. 'plugin out' is password data.
        ByteBuf pluginOut = createAuthenticationDataFor41(plugin, triple.getSecond());

        return createHandshakeResponse41(plugin.getProtocolPluginName(), pluginOut)
                // send response handshake packet
                .flatMap(this::sendHandshakePacket)
                .then(Mono.defer(this::receivePayload))
                // handle authentication Negotiation
                .flatMap(packet -> handleAuthResponse(packet, plugin, triple.getThird()))
                .doOnSuccess(v -> this.connectionPhase.compareAndSet(AUTHENTICATION_PHASE, CONFIGURE_SESSION_PHASE))
                .then()
                ;
    }

    /**
     * <p>
     * must invoke after {@link #authenticateAndInitializing()}
     * </p>
     * configure below url config session group properties:
     * <ul>
     *     <li>{@link PropertyKey#sessionVariables}</li>
     *     <li>{@link PropertyKey#characterEncoding}</li>
     *     <li>{@link PropertyKey#characterSetResults}</li>
     *     <li>{@link PropertyKey#connectionCollation}</li>
     * </ul>
     * <p>
     *     If {@link PropertyKey#detectCustomCollations} is true,then firstly handle this.
     * </p>
     */
    Mono<Void> configureSession() {
        final int phase = this.connectionPhase.get();
        if (phase != CONFIGURE_SESSION_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        return detectCustomCollations()//1.
                .then(Mono.defer(this::executeSetSessionVariables))// 2.
                .then(Mono.defer(this::configureSessionCharset))//3.
                .then(Mono.defer(this::configZoneOffsets)) //4
                .then(Mono.defer(this::configureSessionSuccessEvent))//5.
                ;
    }

    /**
     * <p>
     * must invoke after {@link #configureSession()}
     * </p>
     * <p>
     * do initialize ,must contain below operation:
     *     <ul>
     *         <li>{@code SET autocommit = 0}</li>
     *         <li>{@code SET SESSION TRANSACTION READ COMMITTED}</li>
     *         <li>more initializing operations</li>
     *     </ul>
     * </p>
     *
     * @see #authenticateAndInitializing()
     */
    Mono<Void> initialize() {
        final int phase = this.connectionPhase.get();
        if (phase != INITIALIZING_PHASE) {
            return createConnectionPhaseNotMatchException(phase);
        }
        final String autoCommitCommand = "SET autocommit = 0";
        final String isolationCommand = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED";
        return commandUpdate(autoCommitCommand, EMPTY_STATE_CONSUMER)
                .doOnSuccess(rows -> LOG.debug("Command [{}] execute success.", autoCommitCommand))
                // blow 2 step
                .then(Mono.defer(() -> commandUpdate(isolationCommand, EMPTY_STATE_CONSUMER)))
                .doOnSuccess(rows -> LOG.debug("Command [{}]  execute success.", isolationCommand))
                .doOnSuccess(n -> this.connectionPhase.compareAndSet(INITIALIZING_PHASE, COMMAND_PHASE))
                .then()
                ;
    }



    MySQLUrl getMySQLUrl() {
        return this.mySQLUrl;
    }

    Connection getConnection() {
        return this.connection;
    }

    MySQLCumulateReceiver getCumulateReceiver() {
        return this.cumulateReceiver;
    }

    HandshakeV10Packet getHandshakeV10Packet() {
        return obtainHandshakeV10Packet();
    }

    int getNegotiatedCapability() {
        return obtainNegotiatedCapability();
    }


    @Override
    ZoneOffset obtainDatabaseZoneOffset() {
        return MySQLTimeUtils.toZoneOffset(ZoneId.systemDefault());
    }

    @Override
    int obtainNegotiatedCapability() {
        Integer clientCapability = this.negotiatedCapability.get();
        if (clientCapability == null) {
            clientCapability = createNegotiatedCapability();
            if (!this.negotiatedCapability.compareAndSet(null, clientCapability)) {
                clientCapability = this.negotiatedCapability.get();
            }
        }
        return clientCapability;
    }

    @Override
    Charset obtainCharsetClient() {
        Charset charsetClient = this.charsetClient.get();
        if (charsetClient == null) {
            charsetClient = this.handshakeCharset;
        }
        return charsetClient;
    }

    @Override
    int obtainMaxBytesPerCharClient() {
        return (int) obtainCharsetClient().newEncoder().maxBytesPerChar();
    }

    @Override
    Charset obtainCharsetResults() {
        Charset charsetClient = this.charsetResults.get();
        if (charsetClient == null) {
            charsetClient = StandardCharsets.UTF_8;
        }
        return charsetClient;
    }

    @Override
    Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap() {
        Map<Integer, CharsetMapping.CustomCollation> map;
        map = this.customCollationMap.get();
        return (map != null && this.customCharsetNameSet.get() == null)// detect custom collation two phase finish.
                ? map
                : Collections.emptyMap();
    }

    /*################################## blow private method ##################################*/

    /**
     * @see #authenticate()
     */
    private Mono<Void> handleAuthResponse(ByteBuf payloadBuf, AuthenticationPlugin plugin
            , Map<String, AuthenticationPlugin> pluginMap) {

        Mono<Void> mono;
        if (this.authCounter.addAndGet(1) >= 100) {
            mono = Mono.error(new JdbdMySQLException("TooManyAuthenticationPluginNegotiations"));
        } else if (OkPacket.isOkPacket(payloadBuf)) {
            OkPacket packet = OkPacket.readPacket(payloadBuf, obtainNegotiatedCapability());
            setServerStatus(packet.getStatusFags(), true);
            LOG.debug("MySQL authentication success,info:{}", packet.getInfo());
            mono = Mono.empty();
        } else if (ErrorPacket.isErrorPacket(payloadBuf)) {
            ErrorPacket packet;
            if (this.handshakeSequenceId.get() < 2) {
                packet = ErrorPacket.readPacketInHandshakePhase(payloadBuf);
            } else {
                packet = ErrorPacket.readPacket(payloadBuf, obtainNegotiatedCapability());
            }
            mono = rejectPacket(MySQLExceptionUtils.createErrorPacketException(packet));
        } else {
            mono = processNextAuthenticationNegotiation(payloadBuf, plugin, pluginMap);
        }
        return mono;
    }

    private Mono<Void> processNextAuthenticationNegotiation(ByteBuf payloadBuf, AuthenticationPlugin plugin
            , Map<String, AuthenticationPlugin> pluginMap) {
        final AuthenticationPlugin authPlugin;
        if (PacketUtils.isAuthSwitchRequestPacket(payloadBuf)) {
            payloadBuf.skipBytes(1); // skip type header
            String pluginName = PacketUtils.readStringTerm(payloadBuf, StandardCharsets.US_ASCII);
            LOG.debug("Auth switch request method[{}]", pluginName);
            if (plugin.getProtocolPluginName().equals(pluginName)) {
                authPlugin = plugin;
            } else {
                authPlugin = pluginMap.get(pluginName);
                if (authPlugin == null) {
                    return Mono.error(new JdbdMySQLException("BadAuthenticationPlugin[%s] from server.", pluginName));
                }
            }
            authPlugin.reset();
        } else {
            authPlugin = plugin;
            payloadBuf.skipBytes(1); // skip type header
        }
        // plugin auth
        return Flux.fromIterable(authPlugin.nextAuthenticationStep(payloadBuf))
                .map(this::convertPayloadBufToPacketBuf)
                // all packet send to server
                .flatMap(this::sendHandshakePacket)
                .then(Mono.defer(this::receivePayload))
                // recursion invoke handleAuthResponse
                .flatMap(buffer -> handleAuthResponse(buffer, authPlugin, pluginMap));
    }

    private ByteBuf convertPayloadBufToPacketBuf(ByteBuf payloadBuf) {
        ByteBuf packetBuf = createPacketBuffer(payloadBuf.readableBytes());
        payloadBuf.readBytes(packetBuf);
        payloadBuf.release();
        return packetBuf;
    }

    private void setServerStatus(int serverStatus, boolean saveOldStatusFlags) {
        if (saveOldStatusFlags) {
            this.oldServerStatus.set(this.serverStatus.get());
        } else {
            this.oldServerStatus.set(null);
        }
        this.serverStatus.set(ServerStatus.fromValue(serverStatus));
    }

    private Mono<HandshakeV10Packet> handleHandshakeV10Packet(HandshakeV10Packet packet) {
        if (!this.handshakeV10Packet.compareAndSet(null, packet)) {
            return Mono.error(new JdbdMySQLException(
                    "%s can't concurrently invoke.handshakeV10Packet isn't null.", this));
        }
        final byte handshakeCollationIndex;
        handshakeCollationIndex = obtainHandshakeCollationIndex(this.handshakeCharset, packet.getServerVersion());
        if (!this.handshakeCollationIndex.compareAndSet(0, handshakeCollationIndex)) {
            return Mono.error(new JdbdMySQLException(
                    "%s can't concurrently invoke.handshakeCollationIndex expected[0] but not.", this));
        }
        return this.connectionPhase.compareAndSet(HANDSHAKE_PHASE, SSL_PHASE)
                ? Mono.just(packet)
                : createConcurrentlyConnectionException(HANDSHAKE_PHASE);
    }


    private Mono<HandshakeV10Packet> readHandshakeV10Packet(ByteBuf payloadBuf) {
        if (ErrorPacket.isErrorPacket(payloadBuf)) {
            // reject packet
            ErrorPacket errorPacket = ErrorPacket.readPacket(payloadBuf, 0);
            return FutureMono.from(this.connection.channel().close())
                    .then(Mono.defer(() ->
                            Mono.error(new JdbdMySQLException(
                                    "handshake packet is error packet,close connection.ErrorPacket[%s]", errorPacket))
                    ));
        }
        return Mono.just(HandshakeV10Packet.readHandshake(payloadBuf));
    }

    /**
     * @return read-only {@link ByteBuf}
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41">Protocol::HandshakeResponse41</a>
     */
    private Mono<ByteBuf> createHandshakeResponse41(String authPluginName, ByteBuf pluginOut) {
        final Charset clientCharset = this.handshakeCharset;
        final int clientFlag = obtainNegotiatedCapability();

        final ByteBuf packetBuffer = createPacketBuffer(1024);

        // 1. client_flag,Capabilities Flags, CLIENT_PROTOCOL_41 always set.
        PacketUtils.writeInt4(packetBuffer, clientFlag);
        // 2. max_packet_size
        PacketUtils.writeInt4(packetBuffer, MAX_PACKET_SIZE);
        // 3. character_set
        PacketUtils.writeInt1(packetBuffer, obtainHandshakeCollationIndex());
        // 4. filler,Set of bytes reserved for future use.
        packetBuffer.writeZero(23);

        // 5. username,login user name
        HostInfo hostInfo = getMainHostInfo();
        String user = hostInfo.getUser();
        PacketUtils.writeStringTerm(packetBuffer, user.getBytes(clientCharset));

        // 6. auth_response or (auth_response_length and auth_response)
        if ((clientFlag & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            PacketUtils.writeStringLenEnc(packetBuffer, pluginOut);
        } else {
            packetBuffer.writeByte(pluginOut.readableBytes());
            packetBuffer.writeBytes(pluginOut);
        }
        pluginOut.release();

        // 7. database
        if ((clientFlag & CLIENT_CONNECT_WITH_DB) != 0) {
            String database = this.mySQLUrl.getOriginalDatabase();
            if (!MySQLStringUtils.hasText(database)) {
                throw new JdbdMySQLException("client flag error,check this.getClientFlat() method.");
            }
            PacketUtils.writeStringTerm(packetBuffer, database.getBytes(clientCharset));
        }
        // 8. client_plugin_name
        if ((clientFlag & CLIENT_PLUGIN_AUTH) != 0) {
            PacketUtils.writeStringTerm(packetBuffer, authPluginName.getBytes(clientCharset));
        }
        // 9. client connection attributes
        if ((clientFlag & CLIENT_CONNECT_ATTRS) != 0) {
            Map<String, String> propertySource = createConnectionAttributes();
            // length of all key-values,affected rows
            PacketUtils.writeIntLenEnc(packetBuffer, propertySource.size());
            for (Map.Entry<String, String> e : propertySource.entrySet()) {
                // write key
                PacketUtils.writeStringLenEnc(packetBuffer, e.getKey().getBytes(clientCharset));
                // write value
                PacketUtils.writeStringLenEnc(packetBuffer, e.getValue().getBytes(clientCharset));
            }

        }
        //TODO 10.zstd_compression_level,compression level for zstd compression algorithm
        //packetBuffer.writeByte(0);
        return Mono.just(packetBuffer);
    }

    /**
     * create ssl request packet.
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html#sect_protocol_connection_phase_initial_handshake_ssl_handshake">Protocol::SSL Handshake</a>
     */
    private ByteBuf createSslRequest41Packet() {
        ByteBuf packetBuf = createPacketBuffer(32);
        // 1. client_flag
        PacketUtils.writeInt4(packetBuf, obtainNegotiatedCapability());
        // 2. max_packet_size
        PacketUtils.writeInt4(packetBuf, MAX_PACKET_SIZE);
        // 3. character_set,
        PacketUtils.writeInt1(packetBuf, obtainHandshakeCollationIndex());
        // 4. filler
        packetBuf.writeZero(23);
        return packetBuf;
    }

    private Mono<Void> performSslHandshake() {

        Channel channel = this.connection.channel();
        final SslHandler sslHandler = createSslHandler(channel);

        ChannelPipeline pipeline = channel.pipeline();

        if (pipeline.get(NettyPipeline.ProxyHandler) != null) {
            pipeline.addAfter(NettyPipeline.ProxyHandler, NettyPipeline.SslHandler, sslHandler);
        } else if (pipeline.get(NettyPipeline.ProxyProtocolReader) != null) {
            pipeline.addAfter(NettyPipeline.ProxyProtocolReader, NettyPipeline.SslHandler, sslHandler);
        } else {
            pipeline.addFirst(NettyPipeline.SslHandler, sslHandler);
        }
        return Mono.empty();
    }

    private SslHandler createSslHandler(Channel channel) {
        TrustManagerFactory trustManagerFactory = tryObtainTrustManagerFactory();
        KeyManagerFactory keyManagerFactory = tryObtainKeyManagerFactory();

        SslContextBuilder contextBuilder = SslContextBuilder.forClient();
        if (trustManagerFactory != null) {
            contextBuilder.trustManager(trustManagerFactory);
        }
        if (keyManagerFactory != null) {
            contextBuilder.keyManager(keyManagerFactory);
        }
        contextBuilder.protocols(obtainAllowedTlsProtocolList())
        //TODO zoro 找到确认 jdk or open ssl 支持 cipher suit 的方法
        //.ciphers(obtainAllowedCipherSuitList())
        ;
        try {
            HostInfo hostInfo = getMainHostInfo();
            return contextBuilder
                    .build()
                    .newHandler(channel.alloc(), hostInfo.getHost(), hostInfo.getPort());
        } catch (SSLException e) {
            throw new JdbdMySQLException(e, "create %s", SslContext.class.getName());
        }
    }


    private List<String> obtainAllowedCipherSuitList() {
        String enabledSSLCipherSuites = this.properties.getProperty(PropertyKey.enabledSSLCipherSuites);
        List<String> candidateList = MySQLStringUtils.spitAsList(enabledSSLCipherSuites, ",");

        if (candidateList.isEmpty()) {
            candidateList = ProtocolUtils.CLIENT_SUPPORT_TLS_CIPHER_LIST;
        } else {
            candidateList.retainAll(ProtocolUtils.CLIENT_SUPPORT_TLS_CIPHER_LIST);
        }

        List<String> allowedCipherList = new ArrayList<>(candidateList.size());
        candidateFor:
        for (String candidate : candidateList) {
            for (String restricted : ProtocolUtils.MYSQL_RESTRICTED_CIPHER_SUBSTR_LIST) {
                if (candidate.contains(restricted)) {
                    continue candidateFor;
                }
            }
            allowedCipherList.add(candidate);
        }
        return Collections.unmodifiableList(allowedCipherList);
    }


    /**
     * @return a unmodifiable list
     */
    private List<String> obtainAllowedTlsProtocolList() {
        String enabledTLSProtocols = this.properties.getProperty(PropertyKey.enabledTLSProtocols);
        List<String> candidateList = MySQLStringUtils.spitAsList(enabledTLSProtocols, ",");

        if (candidateList.isEmpty()) {
            ServerVersion serverVersion = obtainHandshakeV10Packet().getServerVersion();

            if (serverVersion.meetsMinimum(5, 7, 28)
                    || (serverVersion.meetsMinimum(5, 6, 46) && !serverVersion.meetsMinimum(5, 7, 0))
                    || (serverVersion.meetsMinimum(5, 6, 0) && ServerVersion.isEnterpriseEdition(serverVersion))) {
                candidateList = ProtocolUtils.CLIENT_SUPPORT_TLS_PROTOCOL_LIST;
            } else {
                candidateList = Collections.unmodifiableList(Arrays.asList(ProtocolUtils.TLSv1_1, ProtocolUtils.TLSv1));
            }
        } else {
            candidateList.retainAll(ProtocolUtils.CLIENT_SUPPORT_TLS_PROTOCOL_LIST);
            candidateList = Collections.unmodifiableList(candidateList);
        }
        return candidateList;
    }

    @Nullable
    private TrustManagerFactory tryObtainTrustManagerFactory() {
        PropertyDefinitions.SslMode sslMode;
        sslMode = this.properties.getProperty(PropertyKey.sslMode, PropertyDefinitions.SslMode.class);
        if (sslMode != PropertyDefinitions.SslMode.VERIFY_CA
                && sslMode != PropertyDefinitions.SslMode.VERIFY_IDENTITY) {
            return null;
        }
        try {
            Pair<KeyStore, char[]> storePair = tryObtainKeyStorePasswordPairForSsl(false);
            if (storePair == null) {
                return null;
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(storePair.getFirst());
            return tmf;
        } catch (NoSuchAlgorithmException e) {
            // use default algorithm, never here.
            throw new JdbdMySQLException(e, "%s algorithm error", TrustManagerFactory.class.getName());
        } catch (KeyStoreException e) {
            throw new JdbdMySQLException(e, "%s content error,cannot init %s."
                    , PropertyKey.trustCertificateKeyStoreUrl, TrustManagerFactory.class.getName());
        }
    }

    @Nullable
    private KeyManagerFactory tryObtainKeyManagerFactory() {
        try {
            Pair<KeyStore, char[]> storePair = tryObtainKeyStorePasswordPairForSsl(true);
            if (storePair == null) {
                return null;
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(storePair.getFirst(), storePair.getSecond());
            return kmf;
        } catch (NoSuchAlgorithmException e) {
            // use default algorithm, never here.
            throw new JdbdMySQLException(e, "%s algorithm error", KeyManagerFactory.class.getName());
        } catch (KeyStoreException | UnrecoverableKeyException e) {
            throw new JdbdMySQLException(e, "%s content error,cannot init %s."
                    , PropertyKey.clientCertificateKeyStoreUrl, KeyManagerFactory.class.getName());
        }
    }

    @Nullable
    private Pair<KeyStore, char[]> tryObtainKeyStorePasswordPairForSsl(final boolean client) {
        // 1. below obtain three storeUrl,storeType,storePassword
        final PropertyKey storeUrlKey, storeTypeKey, passwordKey;
        final String defaultStoreUrlKey, defaultStoreTypeKey, defaultPasswordKey;
        if (client) {
            storeUrlKey = PropertyKey.clientCertificateKeyStoreUrl;
            storeTypeKey = PropertyKey.clientCertificateKeyStoreType;
            passwordKey = PropertyKey.clientCertificateKeyStorePassword;

            defaultStoreUrlKey = "javax.net.ssl.keyStore";
            defaultStoreTypeKey = "javax.net.ssl.keyStoreType";
            defaultPasswordKey = "javax.net.ssl.keyStorePassword";
        } else {
            storeUrlKey = PropertyKey.trustCertificateKeyStoreUrl;
            storeTypeKey = PropertyKey.trustCertificateKeyStoreType;
            passwordKey = PropertyKey.trustCertificateKeyStorePassword;

            defaultStoreUrlKey = "javax.net.ssl.trustStore";
            defaultStoreTypeKey = "javax.net.ssl.trustStoreType";
            defaultPasswordKey = "javax.net.ssl.trustStorePassword";
        }

        final Properties properties = this.properties;
        String storeUrl, storeType;
        storeUrl = properties.getProperty(storeUrlKey);
        storeType = properties.getProperty(storeTypeKey);

        if (!MySQLStringUtils.hasText(storeUrl)) {
            storeUrl = System.getProperty(defaultStoreUrlKey);
        }
        if (!MySQLStringUtils.hasText(storeType)) {
            storeType = System.getProperty(defaultStoreTypeKey);
        }

        if (!MySQLStringUtils.hasText(storeUrl) || !MySQLStringUtils.hasText(storeType)) {
            return null;
        }
        String storePwd = properties.getProperty(passwordKey);
        if (!MySQLStringUtils.hasText(storePwd)) {
            storePwd = System.getenv(defaultPasswordKey);
        }

        final char[] storePassword = (storePwd == null) ? new char[0] : storePwd.toCharArray();
        // 2. create and init KeyStore with three storeUrl,storeType,storePassword
        try (InputStream storeInput = new URL(storeUrl).openStream()) {
            try {
                KeyStore keyStore = KeyStore.getInstance(storeType);
                keyStore.load(storeInput, storePassword);
                return new Pair<>(keyStore, storePassword);
            } catch (KeyStoreException e) {
                throw new JdbdMySQLException(e, "%s[%s] error,cannot create %s.", storeTypeKey, storeType
                        , KeyStore.class.getName());
            } catch (NoSuchAlgorithmException | IOException | CertificateException e) {
                throw new JdbdMySQLException(e, "%s[%s] content error,cannot init %s."
                        , storeUrlKey, storeUrl, KeyStore.class.getName());
            }
        } catch (MalformedURLException e) {
            throw new JdbdMySQLException(e, "%s [%s] isn't url.", storeUrlKey, storeUrl);
        } catch (IOException e) {
            throw new JdbdMySQLException(e, "%s [%s] cannot open InputStream.", storeUrlKey, storeUrl);
        }
    }


    private Mono<Void> disableMultiStatement() {
        return Mono.empty();
    }

    private Mono<PropertyDefinitions.SslMode> doAfterSendSslRequestEnd() {
        return this.connectionPhase.compareAndSet(SSL_PHASE, AUTHENTICATION_PHASE)
                ? Mono.justOrEmpty(this.properties.getProperty(PropertyKey.sslMode, PropertyDefinitions.SslMode.class))
                : createConcurrentlyConnectionException(SSL_PHASE);
    }

    /**
     * @see #configureSession()
     */
    private Mono<Void> detectCustomCollations() {
        if (!this.properties.getRequiredProperty(PropertyKey.detectCustomCollations, Boolean.class)) {
            return Mono.empty();
        }
        // blow tow phase: SHOW COLLATION phase and SHOW CHARACTER SET phase
        return commandQuery("SHOW COLLATION", ORIGINAL_ROW_DECODER, EMPTY_STATE_CONSUMER)
                .filter(this::isCustomCollation)
                .collectMap(this::customCollationMapKeyFunction, this::customCollationMapValueFunction)
                .doOnNext(this::createCustomCollationMapForShowCollation)
                // above SHOW COLLATION phase,blow SHOW CHARACTER SET phase
                .then(Mono.defer(this::detectCustomCharset));
    }

    /**
     * execute config session variables in {@link PropertyKey#sessionVariables}
     *
     * @see #configureSession()
     */
    private Mono<Void> executeSetSessionVariables() {
        String pairString = this.properties.getProperty(PropertyKey.sessionVariables);
        if (!MySQLStringUtils.hasText(pairString)) {
            return Mono.empty();
        }
        String command = ProtocolUtils.buildSetVariableCommand(pairString);
        if (LOG.isDebugEnabled()) {
            LOG.debug("execute set session variables:{}", command);
        }
        return commandUpdate(command, EMPTY_STATE_CONSUMER)
                .then()
                ;

    }


    /**
     * config three session variables:
     * <u>
     * <li>PropertyKey#characterEncoding</li>
     * <li>PropertyKey#characterSetResults</li>
     * <li>PropertyKey#connectionCollation</li>
     * </u>
     *
     * @see #configureSession()
     */
    private Mono<Void> configureSessionCharset() {

        CharsetMapping.Collation connectionCollation = tryObtainConnectionCollation();
        final Charset clientCharset;
        final String namesCommand;
        if (connectionCollation != null) {
            namesCommand = String.format("SET NAMES '%s' COLLATE '%s'"
                    , connectionCollation.mySQLCharset.charsetName
                    , connectionCollation.collationName);
            clientCharset = Charset.forName(connectionCollation.mySQLCharset.javaEncodingsUcList.get(0));
        } else {
            Pair<CharsetMapping.MySQLCharset, Charset> clientPair = obtainClientMySQLCharset();
            namesCommand = String.format("SET NAMES '%s'", clientPair.getFirst().charsetName);
            clientCharset = clientPair.getSecond();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("config session charset:{}", namesCommand);
        }
        // tow phase : SET NAMES and SET character_set_results = ?
        //below one phase SET NAMES
        return commandUpdate(namesCommand, EMPTY_STATE_CONSUMER)
                .doOnSuccess(rows -> commandSetNamesSuccessEvent(clientCharset))
                // below tow phase SET character_set_results = ?
                .then(Mono.defer(this::configCharsetResults))
                ;
    }

    /**
     * config {@link #zoneOffsetClient} and {@link #zoneOffsetDatabase}
     *
     * @see #configureSession()
     */
    private Mono<Void> configZoneOffsets() {
        //1. blow config zoneOffsetClient
        String zoneOffsetClientText = this.properties.getProperty(PropertyKey.zoneOffsetClient);
        ZoneOffset zoneOffsetClient;
        if (MySQLStringUtils.hasText(zoneOffsetClientText)) {
            try {
                zoneOffsetClient = ZoneOffset.of(zoneOffsetClientText);
            } catch (DateTimeException e) {
                return Mono.error(new JdbdMySQLException(e, "%s format error.", PropertyKey.zoneOffsetClient));
            }
        } else {
            zoneOffsetClient = MySQLTimeUtils.systemZoneOffset();
        }
        this.zoneOffsetClient.set(zoneOffsetClient);
        //2. blow config zoneOffsetDatabase
        return performanceZoneOffsetSession()
                .then(Mono.defer(this::retrieveAndConvertDatabaseTimeZone))
                // 3. blow check zoneOffsetClient and zoneOffsetDatabase
                .then(Mono.defer(this::checkAndAcceptZoneOffsets))
                ;
    }

    private Mono<Void> checkAndAcceptZoneOffsets() {
        ZoneOffset zoneOffsetClient, zoneOffsetDatabase;
        zoneOffsetClient = this.zoneOffsetClient.get();
        zoneOffsetDatabase = this.zoneOffsetDatabase.get();
        if (zoneOffsetClient == null || zoneOffsetDatabase == null) {
            return Mono.error(new JdbdMySQLException(
                    "Config ZoneOffset failure,zoneOffsetClient[%s],zoneOffsetDatabase[%s]"
                    , zoneOffsetClient, zoneOffsetDatabase));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("zoneOffsetClient:[{}] ; zoneOffsetDatabase[{}]", zoneOffsetClient, zoneOffsetDatabase);
        }
        return Mono.empty();
    }

    /**
     * @see #configZoneOffsets() (back to invoker)
     */
    private Mono<Void> performanceZoneOffsetSession() {
        String zoneOffsetSessionText = this.properties.getProperty(PropertyKey.zoneOffsetSession);
        Mono<Void> mono;
        if (MySQLStringUtils.hasText(zoneOffsetSessionText)) {
            try {
                ZoneOffset zoneOffsetSession = ZoneOffset.of(zoneOffsetSessionText);
                String command = String.format("SET @@SESSION.time_zone = '%s'", zoneOffsetSession.getId());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("performant: {}", command);
                }
                mono = commandUpdate(command, EMPTY_STATE_CONSUMER)
                        .then()
                ;
            } catch (DateTimeException e) {
                mono = Mono.error(new JdbdMySQLException(e, "%s format error.", PropertyKey.zoneOffsetSession));
            }

        } else {
            mono = Mono.empty();
        }
        return mono;
    }

    /**
     * @see #configZoneOffsets()
     */
    private Mono<Void> retrieveAndConvertDatabaseTimeZone() {
        return commandQuery("SELECT @@SESSION.time_zone as timeZone", ORIGINAL_ROW_DECODER, EMPTY_STATE_CONSUMER)
                .elementAt(0)
                .flatMap(this::handleDatabaseTimeZone)
                ;
    }

    /**
     * @see #configZoneOffsets()
     */
    private Mono<Void> handleDatabaseTimeZone(ResultRow resultRow) {
        String databaseZoneText = resultRow.getRequiredObject("timeZone", String.class);
        Mono<Void> mono;
        if ("SYSTEM".equalsIgnoreCase(databaseZoneText)) {
            mono = convertDatabaseSystemZone();
        } else {
            try {
                ZoneId databaseZone = ZoneId.of(databaseZoneText, ZoneId.SHORT_IDS);
                this.zoneOffsetDatabase.set(MySQLTimeUtils.toZoneOffset(databaseZone));
                mono = Mono.empty();
            } catch (DateTimeException e) {
                mono = Mono.error(new JdbdMySQLException(e, "MySQL time_zone[%s] cannot convert to %s ."
                        , databaseZoneText, ZoneId.class.getName()));
            }
        }
        return mono;
    }

    /**
     * @see #handleDatabaseTimeZone(ResultRow)
     */
    private Mono<Void> convertDatabaseSystemZone() {
        final long utcEpochSecond = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond();
        String command = String.format("SELECT from_unixtime(%s) as databaseNow;", utcEpochSecond);
        if (LOG.isDebugEnabled()) {
            LOG.debug("convert time_zone[SYSTEM],execute command: {}", command);
        }
        return commandQuery(command, ORIGINAL_ROW_DECODER, EMPTY_STATE_CONSUMER)
                .elementAt(0)
                .doOnNext(row -> handleDatabaseNowForConvertDatabaseZone(row, utcEpochSecond))
                .then()
                ;
    }

    /**
     * @see #convertDatabaseSystemZone()
     */
    private void handleDatabaseNowForConvertDatabaseZone(ResultRow resultRow, final long utcEpochSecond) {
        LocalDateTime dateTimeOfDatabase;
        dateTimeOfDatabase = LocalDateTime.parse(
                resultRow.getRequiredObject("databaseNow", String.class), MYSQL_DATETIME_FORMATTER);
        OffsetDateTime dateTime = OffsetDateTime.of(dateTimeOfDatabase, ZoneOffset.UTC);
        int totalSeconds = (int) (dateTime.toEpochSecond() - utcEpochSecond);
        this.zoneOffsetDatabase.set(ZoneOffset.ofTotalSeconds(totalSeconds));
    }


    /**
     * @see #configureSessionCharset()
     * @see #commandSetNamesSuccessEvent(Charset)
     */
    private Mono<Void> configCharsetResults() {
        final String charset = this.properties.getProperty(PropertyKey.characterSetResults, String.class);
        if (charset != null && !MySQLStringUtils.hasText(charset)) {
            return Mono.empty();
        }
        final Charset charsetResults;
        final String command;
        if (charset == null || charset.equalsIgnoreCase("NULL")) {
            command = "SET character_set_results = NULL";
            charsetResults = null;
        } else {
            ServerVersion serverVersion = obtainHandshakeV10Packet().getServerVersion();
            CharsetMapping.MySQLCharset mySQLCharset;
            mySQLCharset = CharsetMapping.getMysqlCharsetForJavaEncoding(charset, serverVersion);
            if (mySQLCharset == null) {
                return Mono.error(new JdbdMySQLException(
                        "No found MySQL charset fro Property characterSetResults[%s]", charset));
            }
            command = String.format("SET character_set_results = '%s'", mySQLCharset.charsetName);
            charsetResults = Charset.forName(charset);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("config charset result:{}", command);
        }
        return commandUpdate(command, EMPTY_STATE_CONSUMER)
                .flatMap(rows -> overrideCharsetResults(charsetResults));

    }

    /**
     * override {@link #charsetResults}
     *
     * @see #configCharsetResults()
     */
    private Mono<Void> overrideCharsetResults(@Nullable Charset charsetResults) {
        if (charsetResults != null) {
            this.charsetResults.set(charsetResults);
            return Mono.empty();
        }
        String command = "SHOW VARIABLES WHERE Variable_name = 'character_set_system'";
        if (LOG.isDebugEnabled()) {
            LOG.debug("override charsetResults,execute command: {}", command);
        }
        return commandQuery(command, ORIGINAL_ROW_DECODER, EMPTY_STATE_CONSUMER)
                .elementAt(0)
                .doOnNext(this::updateCharsetResultsAfterQueryCharacterSetSystem)
                .then()
                ;
    }

    /**
     * update {@link #charsetResults}
     *
     * @see #overrideCharsetResults(Charset)
     */
    private void updateCharsetResultsAfterQueryCharacterSetSystem(ResultRow resultRow) {
        final String charset = resultRow.getObject("Value", String.class);
        if (charset == null || charset.equalsIgnoreCase("NULL")) {
            this.charsetResults.set(StandardCharsets.UTF_8);
        } else {
            ServerVersion serverVersion = obtainHandshakeV10Packet().getServerVersion();
            CharsetMapping.MySQLCharset mySQLCharset;
            mySQLCharset = CharsetMapping.getMysqlCharsetForJavaEncoding(charset, serverVersion);
            if (mySQLCharset == null) {
                throw new JdbdMySQLException(
                        "Not found java charset for character_set_system[%s]", charset);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("override charsetResults with character_set_system:{}", charset);
            }
            this.charsetResults.set(Charset.forName(mySQLCharset.javaEncodingsUcList.get(0)));
        }
    }


    /**
     * @see #configureSessionCharset()
     * @see #configCharsetResults()
     */
    private void commandSetNamesSuccessEvent(Charset clientCharset) {
        this.charsetClient.set(clientCharset);
        this.charsetResults.set(clientCharset);
    }


    @Nullable
    private CharsetMapping.Collation tryObtainConnectionCollation() {
        CharsetMapping.Collation collationConnection;
        String connectionCollation = this.properties.getProperty(PropertyKey.connectionCollation, String.class);
        if (!MySQLStringUtils.hasText(connectionCollation)) {
            return null;
        }
        collationConnection = CharsetMapping.getCollationByName(connectionCollation);
        if (collationConnection != null
                && CharsetMapping.isUnsupportedCharset(collationConnection.mySQLCharset.charsetName)) {
            collationConnection = CharsetMapping.INDEX_TO_COLLATION.get(CharsetMapping.MYSQL_COLLATION_INDEX_utf8mb4);
        }
        return collationConnection;
    }

    private Pair<CharsetMapping.MySQLCharset, Charset> obtainClientMySQLCharset() {
        final String charsetClient = properties.getProperty(PropertyKey.characterEncoding);

        final Pair<CharsetMapping.MySQLCharset, Charset> defaultPair;
        defaultPair = new Pair<>(
                CharsetMapping.CHARSET_NAME_TO_CHARSET.get(CharsetMapping.utf8mb4)
                , StandardCharsets.UTF_8
        );

        Pair<CharsetMapping.MySQLCharset, Charset> pair = null;
        if (!MySQLStringUtils.hasText(charsetClient)
                || !CharsetMapping.isUnsupportedCharset(charsetClient)
                || StandardCharsets.UTF_8.name().equalsIgnoreCase(charsetClient)
                || StandardCharsets.UTF_8.aliases().contains(charsetClient)) {
            pair = defaultPair;
        } else {
            ServerVersion serverVersion = obtainHandshakeV10Packet().getServerVersion();
            CharsetMapping.MySQLCharset mySQLCharset = CharsetMapping.getMysqlCharsetForJavaEncoding(
                    charsetClient, serverVersion);
            if (mySQLCharset != null) {
                pair = new Pair<>(mySQLCharset, Charset.forName(charsetClient));
            }
        }

        if (pair == null) {
            pair = defaultPair;
        }
        return pair;
    }


    /**
     * handle {@link #configureSession()} success event.
     *
     * @see #configureSession()
     */
    private Mono<Void> configureSessionSuccessEvent() {
        if (!this.connectionPhase.compareAndSet(CONFIGURE_SESSION_PHASE, INITIALIZING_PHASE)) {
            return createConcurrentlyConnectionException(CONFIGURE_SESSION_PHASE);
        }
        Charset charsetClient = this.charsetClient.get();
        Charset charsetResults = this.charsetResults.get();
        if (charsetClient == null || charsetResults == null) {
            return Mono.error
                    (new JdbdMySQLException("charsetClient:{},charsetResults:{}", charsetClient, charsetResults));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("charsetClient:{},charsetResults:{}", charsetClient, charsetResults);
        }
        return Mono.empty();
    }

    private <T> Mono<T> createConcurrentlyConnectionException(int expected) {
        return Mono.error(new JdbdMySQLException(
                "%s can't concurrently invoke.connectionPhase expected[%s] but not.", this, expected));
    }

    private <T> Mono<T> createConnectionPhaseNotMatchException(int currentPhase) {
        return Mono.error(new JdbdMySQLException("Connection phase not match,current phase[%s]", currentPhase));
    }


    /**
     * @see #authenticate()
     */
    private Triple<AuthenticationPlugin, Boolean, Map<String, AuthenticationPlugin>> obtainAuthenticationPlugin() {
        Map<String, AuthenticationPlugin> pluginMap = loadAuthenticationPluginMap();

        Properties properties = this.properties;
        HandshakeV10Packet handshakeV10Packet = obtainHandshakeV10Packet();
        String pluginName = handshakeV10Packet.getAuthPluginName();

        AuthenticationPlugin plugin = pluginMap.get(pluginName);
        boolean skipPassword = false;
        final boolean useSsl = isUseSsl();
        if (plugin == null) {
            plugin = pluginMap.get(properties.getRequiredProperty(PropertyKey.defaultAuthenticationPlugin));
        } else if (Sha256PasswordPlugin.PLUGIN_NAME.equals(pluginName)
                && !useSsl
                && properties.getProperty(PropertyKey.serverRSAPublicKeyFile) == null
                && !properties.getRequiredProperty(PropertyKey.allowPublicKeyRetrieval, Boolean.class)) {
            /*
             * Fall back to default if plugin is 'sha256_password' but required conditions for this to work aren't met. If default is other than
             * 'sha256_password' this will result in an immediate authentication switch request, allowing for other plugins to authenticate
             * successfully. If default is 'sha256_password' then the authentication will fail as expected. In both cases user's password won't be
             * sent to avoid subjecting it to lesser security levels.
             */
            String defaultPluginName = properties.getRequiredProperty(PropertyKey.defaultAuthenticationPlugin);
            skipPassword = !pluginName.equals(defaultPluginName);
            plugin = pluginMap.get(defaultPluginName);
        }
        if (plugin.requiresConfidentiality() && !useSsl) {
            throw new JdbdMySQLException("AuthenticationPlugin[%s] required SSL", plugin.getClass().getName());
        }
        return new Triple<>(plugin, skipPassword, pluginMap);
    }


    private int createNegotiatedCapability() {
        HandshakeV10Packet handshakeV10Packet = obtainHandshakeV10Packet();
        final int serverCapability = handshakeV10Packet.getCapabilityFlags();
        final Properties env = this.properties;

        final boolean useConnectWithDb = MySQLStringUtils.hasText(this.mySQLUrl.getOriginalDatabase())
                && !env.getRequiredProperty(PropertyKey.createDatabaseIfNotExist, Boolean.class);

        return CLIENT_SECURE_CONNECTION
                | CLIENT_PLUGIN_AUTH
                | (serverCapability & CLIENT_LONG_PASSWORD)  //
                | (serverCapability & CLIENT_PROTOCOL_41)    //

                | (serverCapability & CLIENT_TRANSACTIONS)   // Need this to get server status values
                | (serverCapability & CLIENT_MULTI_RESULTS)  // We always allow multiple result sets
                | (serverCapability & CLIENT_PS_MULTI_RESULTS)  // We always allow multiple result sets for SSPS
                | (serverCapability & CLIENT_LONG_FLAG)      //

                | (serverCapability & CLIENT_DEPRECATE_EOF)  //
                | (serverCapability & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
                | (env.getRequiredProperty(PropertyKey.useCompression, Boolean.class) ? (serverCapability & CLIENT_COMPRESS) : 0)
                | (useConnectWithDb ? (serverCapability & CLIENT_CONNECT_WITH_DB) : 0)
                | (env.getRequiredProperty(PropertyKey.useAffectedRows, Boolean.class) ? 0 : (serverCapability & CLIENT_FOUND_ROWS))

                | (env.getRequiredProperty(PropertyKey.allowLoadLocalInfile, Boolean.class) ? (serverCapability & CLIENT_LOCAL_FILES) : 0)
                | (env.getRequiredProperty(PropertyKey.interactiveClient, Boolean.class) ? (serverCapability & CLIENT_INTERACTIVE) : 0)
                | (env.getRequiredProperty(PropertyKey.allowMultiQueries, Boolean.class) ? (serverCapability & CLIENT_MULTI_STATEMENTS) : 0)
                | (env.getRequiredProperty(PropertyKey.disconnectOnExpiredPasswords, Boolean.class) ? 0 : (serverCapability & CLIENT_CAN_HANDLE_EXPIRED_PASSWORD))

                | (NONE.equals(env.getProperty(PropertyKey.connectionAttributes)) ? 0 : (serverCapability & CLIENT_CONNECT_ATTRS))
                | (env.getRequiredProperty(PropertyKey.sslMode, PropertyDefinitions.SslMode.class) != PropertyDefinitions.SslMode.DISABLED ? (serverCapability & CLIENT_SSL) : 0)

                // TODO MYSQLCONNJ-437
                // clientParam |= (capabilityFlags & NativeServerSession.CLIENT_SESSION_TRACK);

                ;
    }

    private byte obtainHandshakeCollationIndex() {
        int index = this.handshakeCollationIndex.get();
        if (index == 0) {
            throw new IllegalStateException("client no handshake,handshakeCollationIndex no value.");
        }
        return (byte) index;
    }

    /**
     * @return a unmodifiable map
     */
    private Map<String, AuthenticationPlugin> loadAuthenticationPluginMap() {
        Properties properties = this.properties;

        String defaultPluginName = properties.getRequiredProperty(PropertyKey.defaultAuthenticationPlugin);
        List<String> disabledPluginList = properties.getPropertyList(PropertyKey.disabledAuthenticationPlugins);

        // below three line: obtain pluginClassNameList
        List<String> pluginClassNameList = properties.getPropertyList(PropertyKey.authenticationPlugins);
        pluginClassNameList.add(defaultPluginName);
        appendBuildInPluginClassNameList(pluginClassNameList);

        // below create AuthenticationPlugin map
        HostInfo hostInfo = getMainHostInfo();
        Map<String, AuthenticationPlugin> map = new HashMap<>();

        boolean defaultIsFound = false;
        for (String pluginClassName : pluginClassNameList) {
            if (disabledPluginList.contains(pluginClassName)) {
                continue;
            }
            AuthenticationPlugin plugin = loadPlugin(pluginClassName, this, hostInfo);
            String pluginName = plugin.getProtocolPluginName();
            if (disabledPluginList.contains(pluginName)) {
                continue;
            }
            map.put(pluginName, plugin);
            if (pluginClassName.equals(defaultPluginName)) {
                defaultIsFound = true;
            }
        }
        if (!defaultIsFound) {
            throw new JdbdMySQLException("defaultAuthenticationPlugin[%s] not fond or disable.", defaultPluginName);
        }
        return Collections.unmodifiableMap(map);
    }


    private HandshakeV10Packet obtainHandshakeV10Packet() {
        HandshakeV10Packet packet = this.handshakeV10Packet.get();
        if (packet == null) {
            throw new IllegalStateException("no handshake.");
        }
        return packet;
    }


    private ByteBuf createAuthenticationDataFor41(AuthenticationPlugin plugin, boolean skipPassword) {
        ByteBuf payloadBuf;
        if (skipPassword) {
            // skip password
            payloadBuf = createOneSizePayload(0);
        } else {
            HandshakeV10Packet handshakeV10Packet = obtainHandshakeV10Packet();
            String seed = handshakeV10Packet.getAuthPluginDataPart1() + handshakeV10Packet.getAuthPluginDataPart2();
            byte[] seedBytes = seed.getBytes();
            ByteBuf fromServer = createPayloadBuffer(seedBytes.length);
            fromServer.writeBytes(seedBytes);
            // invoke AuthenticationPlugin
            List<ByteBuf> toServer = plugin.nextAuthenticationStep(fromServer);
            // release temp fromServer
            fromServer.release();
            if (toServer.isEmpty()) {
                throw new IllegalStateException(String.format(
                        "AuthenticationPlugin[%s] nextAuthenticationStep return error"
                        , plugin.getClass().getName()));
            }
            payloadBuf = toServer.get(0);
        }
        return payloadBuf;
    }


    private Map<String, String> createConnectionAttributes() {
        String connectionStr = this.properties.getProperty(PropertyKey.connectionAttributes);
        Map<String, String> attMap = new HashMap<>();

        if (connectionStr != null) {
            String[] pairArray = connectionStr.split(",");
            for (String pair : pairArray) {
                String[] kv = pair.split(":");
                if (kv.length != 2) {
                    throw new IllegalStateException(String.format("key[%s] can't resolve pair." +
                            "", PropertyKey.connectionAttributes));
                }
                attMap.put(kv[0].trim(), kv[1].trim());
            }
        }

        // Leaving disabled until standard values are defined
        // props.setProperty("_os", NonRegisteringDriver.OS);
        // props.setProperty("_platform", NonRegisteringDriver.PLATFORM);
        attMap.put("_client_name", Constants.CJ_NAME);
        attMap.put("_client_version", Constants.CJ_VERSION);
        attMap.put("_runtime_vendor", Constants.JVM_VENDOR);
        attMap.put("_runtime_version", Constants.JVM_VERSION);
        attMap.put("_client_license", Constants.CJ_LICENSE);
        return attMap;
    }


    /**
     * packet header have read.
     *
     * @see #readPacketHeader(ByteBuf)
     * @see #sendHandshakePacket(ByteBuf)
     */
    private Mono<ByteBuf> receivePayload() {
        // LOG.debug("receive payload,last sequenceId:{}", this.handshakeSequenceId.get());
        return this.cumulateReceiver.receiveOnePacket()
                .flatMap(this::readPacketHeader);
    }


    /**
     * @see #detectCustomCollations()
     */
    private void createCustomCollationMapForShowCollation(Map<Integer, CharsetMapping.CustomCollation> map) {
        if (map.isEmpty()) {
            this.customCollationMap.set(Collections.emptyMap());
            this.customCharsetNameSet.set(Collections.emptySet());
        } else {

            Set<String> charsetNameSet = new HashSet<>();
            for (CharsetMapping.CustomCollation collation : map.values()) {
                if (!CharsetMapping.CHARSET_NAME_TO_CHARSET.containsKey(collation.charsetName)) {
                    throw new JdbdMySQLException("Custom collation[%s] not found corresponding java charset."
                            , collation.charsetName);
                }
                charsetNameSet.add(collation.charsetName);
            }
            //firstly customCharsetNameSet
            this.customCharsetNameSet.set(Collections.unmodifiableSet(charsetNameSet));
            // secondly customCollationMap
            this.customCollationMap.set(Collections.unmodifiableMap(map));
        }
    }

    private Integer customCollationMapKeyFunction(ResultRow resultRow) {
        return resultRow.getRequiredObject("Id", Integer.class);
    }

    private CharsetMapping.CustomCollation customCollationMapValueFunction(ResultRow resultRow) {
        return new CharsetMapping.CustomCollation(
                resultRow.getRequiredObject("Id", Integer.class)
                , resultRow.getRequiredObject("Collation", String.class)
                , resultRow.getRequiredObject("Charset", String.class)
                , -1 // placeholder.
        );
    }

    /**
     * @see #detectCustomCollations()
     */
    private boolean isCustomCollation(ResultRow resultRow) {
        return !CharsetMapping.INDEX_TO_COLLATION.containsKey(resultRow.getRequiredObject("Id", Integer.class));
    }

    /**
     * @see #detectCustomCollations()
     */
    private Mono<Void> detectCustomCharset() {
        final Set<String> charsetNameSet = this.customCharsetNameSet.get();
        Mono<Void> mono;
        if (charsetNameSet == null) {
            mono = Mono.error(new JdbdMySQLException("no detect custom collation."));
        } else if (charsetNameSet.isEmpty()) {
            mono = Mono.empty();
        } else {

            mono = commandQuery("SHOW CHARACTER SET", ORIGINAL_ROW_DECODER, EMPTY_STATE_CONSUMER)
                    .filter(resultRow -> charsetNameSet.contains(resultRow.getRequiredObject("Charset", String.class)))
                    .collectMap(resultRow -> resultRow.getRequiredObject("Charset", String.class)
                            , resultRow -> resultRow.getRequiredObject("Maxlen", Integer.class))
                    .doOnNext(this::createCustomCollations)
                    .then();
        }
        return mono;
    }


    /**
     * @see #detectCustomCharset()
     */
    private void createCustomCollations(final Map<String, Integer> customCharsetToMaxLenMap) {
        // oldCollationMap no max length of charset.
        Map<Integer, CharsetMapping.CustomCollation> oldCollationMap = this.customCollationMap.get();
        if (oldCollationMap == null) {
            throw new IllegalStateException("No detect custom collation.");
        }
        Map<Integer, CharsetMapping.CustomCollation> newCollationMap = new HashMap<>();
        for (Map.Entry<Integer, CharsetMapping.CustomCollation> e : oldCollationMap.entrySet()) {
            Integer index = e.getKey();
            CharsetMapping.CustomCollation collation = e.getValue();

            String charsetName = collation.charsetName;
            Integer maxLen = customCharsetToMaxLenMap.get(charsetName);
            CharsetMapping.CustomCollation newCollation = new CharsetMapping.CustomCollation(
                    index, collation.collationName, charsetName, maxLen);

            newCollationMap.put(index, newCollation);
        }
        //firstly customCollationMap
        this.customCollationMap.set(Collections.unmodifiableMap(newCollationMap));
        //secondly customCharsetNameSet
        this.customCharsetNameSet.set(null);
    }


    /**
     * @see #receivePayload()
     * @see #sendHandshakePacket(ByteBuf)
     */
    private Mono<ByteBuf> readPacketHeader(ByteBuf packetBuf) {
        packetBuf.skipBytes(3); // skip payload length
        int sequenceId = PacketUtils.readInt1(packetBuf);
        // LOG.debug("receive handshake packet,sequenceId:{}", sequenceId);
        Mono<ByteBuf> mono;
        if (this.handshakeSequenceId.compareAndSet(sequenceId - 1, sequenceId)) {
            mono = Mono.just(packetBuf);
        } else {
            mono = Mono.error(new JdbdMySQLException(
                    "sequenceId[%s] form server error,should be %s .", sequenceId, sequenceId - 1));
        }
        return mono;

    }


    private MySQLRowMeta parseUpdateCommandResultPacket(ByteBuf resultPacket) {
        PacketDecoders.ComQueryResponse response;
        final int negotiatedCapability = obtainNegotiatedCapability();
        response = PacketDecoders.detectComQueryResponseType(resultPacket, negotiatedCapability);
        switch (response) {
            case TEXT_RESULT:
                MySQLColumnMeta[] columnMetas = PacketDecoders.readResultColumnMetas(
                        resultPacket, negotiatedCapability
                        , obtainCharsetResults(), this.properties);
                return MySQLRowMeta.from(columnMetas, obtainCustomCollationMap());
            case ERROR:
                ErrorPacket packet;
                packet = ErrorPacket.readPacket(resultPacket, negotiatedCapability, obtainCharsetResults());
                throw new JdbdMySQLException("Expected result set response,but error,{} ", packet.getErrorMessage());
            default:
                throw new JdbdMySQLException("Expected result set response,but %s ", response);
        }
    }

    /**
     * @see #readPacketHeader(ByteBuf)
     * @see #receivePayload()
     */
    private Mono<Void> sendHandshakePacket(ByteBuf packetBuffer) {
        return sendPacket(packetBuffer, this.handshakeSequenceId);
    }


    private Mono<Void> closeConnection() {
        return FutureMono.from(this.connection.channel().close());
    }

    private <T> Mono<T> rejectPacket(Throwable rejectReason) {
        return closeConnection()
                .then(Mono.error(rejectReason));
    }


    /*################################## blow static method ##################################*/

    private static AuthenticationPlugin loadPlugin(String pluginClassName, ProtocolAssistant assistant
            , HostInfo hostInfo) {
        try {
            Class<?> pluginClass = Class.forName(convertPluginClassName(pluginClassName));
            if (!AuthenticationPlugin.class.isAssignableFrom(pluginClass)) {
                throw new JdbdMySQLException("class[%s] isn't %s type.", AuthenticationPlugin.class.getName());
            }
            Method method = pluginClass.getMethod("getInstance", ProtocolAssistant.class, HostInfo.class);
            if (!AuthenticationPlugin.class.isAssignableFrom(method.getReturnType())) {
                throw new JdbdMySQLException("plugin[%s] getInstance return error type.", pluginClassName);
            }
            return (AuthenticationPlugin) method.invoke(null, assistant, hostInfo);
        } catch (ClassNotFoundException e) {
            throw new JdbdMySQLException(e, "plugin[%s] not found in classpath.", pluginClassName);
        } catch (NoSuchMethodException e) {
            throw new JdbdMySQLException(e, "plugin[%s] no getInstance() factory method.", pluginClassName);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new JdbdMySQLException(e, "plugin[%s] getInstance() invoke error.", pluginClassName);
        }
    }


    private static void appendBuildInPluginClassNameList(List<String> pluginClassNameList) {
        pluginClassNameList.add(MySQLNativePasswordPlugin.class.getName());
        pluginClassNameList.add(MySQLClearPasswordPlugin.class.getName());
        pluginClassNameList.add(Sha256PasswordPlugin.class.getName());
        pluginClassNameList.add(CachingSha2PasswordPlugin.class.getName());

        pluginClassNameList.add(MySQLNativePasswordPlugin.class.getName());

    }

    private static String convertPluginClassName(String pluginClassName) {
        String className;
        switch (pluginClassName) {
            case MySQLNativePasswordPlugin.PLUGIN_NAME:
            case MySQLNativePasswordPlugin.PLUGIN_CLASS:
                className = MySQLNativePasswordPlugin.class.getName();
                break;
            case CachingSha2PasswordPlugin.PLUGIN_NAME:
            case CachingSha2PasswordPlugin.PLUGIN_CLASS:
                className = CachingSha2PasswordPlugin.class.getName();
                break;
            case MySQLClearPasswordPlugin.PLUGIN_NAME:
            case MySQLClearPasswordPlugin.PLUGIN_CLASS:
                className = MySQLClearPasswordPlugin.class.getName();
                break;
            case MySQLOldPasswordPlugin.PLUGIN_NAME:
            case MySQLOldPasswordPlugin.PLUGIN_CLASS:
                className = MySQLOldPasswordPlugin.class.getName();
                break;
            case Sha256PasswordPlugin.PLUGIN_NAME:
            case Sha256PasswordPlugin.PLUGIN_CLASS:
                className = Sha256PasswordPlugin.class.getName();
                break;
            default:
                className = pluginClassName;
        }
        return className;
    }


    private static byte obtainHandshakeCollationIndex(Charset handshakeCharset, ServerVersion serverVersion) {
        int charsetIndex = CharsetMapping.getCollationIndexForJavaEncoding(handshakeCharset.name(), serverVersion);
        if (charsetIndex == 0) {
            charsetIndex = CharsetMapping.MYSQL_COLLATION_INDEX_utf8;
        }
        return (byte) charsetIndex;
    }


}
