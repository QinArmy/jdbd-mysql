package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.env.MySQLHost;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.session.SessionCloseException;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.task.CommunicationTask;
import io.jdbd.vendor.task.ConnectionTask;
import io.jdbd.vendor.task.SslWrapper;
import io.jdbd.vendor.util.Pair;
import io.jdbd.vendor.util.SQLStates;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.SslHandler;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This class is the implementation of MySQL connection protocol.
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html">Connection Phase</a>
 * @since 1.0
 */
final class MySQLConnectionTask extends CommunicationTask implements AuthenticateAssistant, ConnectionTask {


    static Mono<AuthenticateResult> authenticate(final TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                MySQLConnectionTask task = new MySQLConnectionTask(adjutant, sink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    static final String JVM_VENDOR = System.getProperty("java.vendor");
    static final String JVM_VERSION = System.getProperty("java.version");

    private static final Logger LOG = LoggerFactory.getLogger(MySQLConnectionTask.class);

    private final TaskAdjutant adjutant;

    private final ByteBufAllocator allocator;

    private final FixedEnv fixedEnv;

    private final MonoSink<AuthenticateResult> sink;

    private final MySQLHost host;

    private final Environment env;

    private final Map<String, AuthenticationPlugin> pluginMap;

    private Charset handshakeCharset;

    private byte handshakeCollationIndex;

    private int sequenceId = -1;

    private Phase phase;

    private int capability = 0;

    private Handshake10 handshake;

    private AuthenticationPlugin plugin;

    // non-volatile ,because all modify in netty EventLoop .
    private int authCounter = 0;

    private String errorMessage;

    private Consumer<SslWrapper> sslConsumer;

    private MySQLConnectionTask(TaskAdjutant adjutant, MonoSink<AuthenticateResult> sink) {
        super(adjutant, sink::error);
        this.sink = sink;
        this.adjutant = adjutant;
        this.allocator = adjutant.allocator();

        final ClientProtocolFactory factory;
        factory = adjutant.getFactory();

        this.fixedEnv = factory;
        this.host = factory.host;
        this.env = factory.env;


        this.handshakeCharset = parseCharacterEncoding(this.env);

        this.pluginMap = this.createPluginMap();
    }





    /*################################## blow AuthenticateAssistant method ##################################*/

    @Override
    public Charset getHandshakeCharset() {
        return this.handshakeCharset;
    }

    @Override
    public Charset getPasswordCharset() {
        return this.env.get(MySQLKey.PASSWORD_CHARACTER_ENCODING, this::getHandshakeCharset);
    }

    @Override
    public MySQLHost getHostInfo() {
        return this.host;
    }

    @Override
    public boolean isUseSsl() {
        return (this.capability & Capabilities.CLIENT_SSL) != 0;
    }


    @Override
    public MySQLServerVersion getServerVersion() {
        return this.handshake.getServerVersion();
    }

    @Override
    public ByteBufAllocator allocator() {
        return this.adjutant.allocator();
    }

    @Override
    public void addSsl(Consumer<SslWrapper> sslConsumer) {
        this.sslConsumer = sslConsumer;
    }

    @Override
    public boolean reconnect() {
        return false;
    }

    @Override
    public boolean disconnect() {
        return this.phase == Phase.DISCONNECT;
    }

    @Override
    public String toString() {
        String text;
        text = super.toString();
        if (this.errorMessage != null) {
            text = text + "##" + this.errorMessage;
        }
        return text;
    }

    /*################################## blow protected method ##################################*/

    @Nullable
    @Override
    protected Publisher<ByteBuf> start() {
        if (this.phase == null) {
            //may be load plugin occur error.
            this.phase = Phase.RECEIVE_HANDSHAKE;
        }
        // no data to send after receive Handshake packet from server.
        return null;
    }


    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        boolean taskEnd = false, continueDecode = Packets.hasOnePacket(cumulateBuffer);
        while (continueDecode) {
            switch (this.phase) {
                case DISCONNECT: {
                    taskEnd = true;
                    continueDecode = false;
                }
                break;
                case RECEIVE_HANDSHAKE: {
                    receiveHandshakeAndSendResponse(cumulateBuffer);
                    taskEnd = this.phase == Phase.DISCONNECT;
                    continueDecode = false;
                }
                break;
                case HANDSHAKE_RESPONSE: {
                    this.phase = Phase.AUTHENTICATE;
                    taskEnd = authenticateDecode(cumulateBuffer, serverStatusConsumer);
                    continueDecode = !taskEnd && Packets.hasOnePacket(cumulateBuffer);
                }
                break;
                case AUTHENTICATE: {
                    taskEnd = authenticateDecode(cumulateBuffer, serverStatusConsumer);
                    continueDecode = !taskEnd && Packets.hasOnePacket(cumulateBuffer);
                }
                break;
                default:
                    throw new IllegalStateException(String.format("%s this.phase[%s] error.", this, this.phase));
            }
        }
        if (taskEnd && this.phase != Phase.DISCONNECT) {
            this.phase = Phase.END;
        }
        return taskEnd;
    }

    @Override
    protected Action onError(Throwable e) {
        if (this.phase != Phase.END) {
            this.sink.error(MySQLExceptions.wrap(e));
        }
        return Action.TASK_END;
    }

    @Override
    protected void onChannelClose() {
        if (this.phase != Phase.DISCONNECT) {
            handleAuthenticateFailure(new SessionCloseException("Channel unexpected close."));
        }
    }

    /*################################## blow private method ##################################*/


    /**
     * @see #decode(ByteBuf, Consumer)
     */
    private void receiveHandshakeAndSendResponse(final ByteBuf cumulateBuffer) {
        //1. read handshake packet
        final int payloadLength;
        payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
        final Handshake10 handshake;
        handshake = Handshake10.read(cumulateBuffer, payloadLength);
        this.handshake = handshake;

        if (LOG.isDebugEnabled()) {
            LOG.debug("receive handshake success:\n{}", handshake);
        }
        //2. negotiate capabilities
        final int negotiatedCapability;
        negotiatedCapability = createNegotiatedCapability(handshake);
        this.capability = negotiatedCapability;

        //3.create handshake collation index and charset
        int handshakeCollationIndex;
        handshakeCollationIndex = Charsets.getCollationIndexForJavaEncoding(
                this.handshakeCharset.name(), handshake.serverVersion);
        if (handshakeCollationIndex == 0) {
            handshakeCollationIndex = Charsets.MYSQL_COLLATION_INDEX_utf8mb4;
            this.handshakeCharset = StandardCharsets.UTF_8;
        }
        this.handshakeCollationIndex = (byte) handshakeCollationIndex;

        //4. optional ssl request or send plaintext handshake response.
        if ((negotiatedCapability & Capabilities.CLIENT_SSL) != 0) {
            LOG.debug("send ssl request.");
            sendSslRequest();
        } else {
            LOG.debug("plaintext send handshake response.");
        }
        if ((negotiatedCapability & Capabilities.CLIENT_PROTOCOL_41) == 0) {
            String m = String.format("server version[%s] too old", handshake.serverVersion);
            this.addError(new JdbdException(m));
            this.packetPublisher = null;
            this.phase = Phase.DISCONNECT;
        } else {
            this.packetPublisher = createHandshakeResponsePacket();
            this.phase = Phase.HANDSHAKE_RESPONSE;
        }

    }

    /**
     * @return true : task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean authenticateDecode(final ByteBuf cumulateBuffer, final Consumer<Object> serverConsumer) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("decode authenticate packet ,authCounter:{}", this.authCounter);
        }
        assert this.phase == Phase.AUTHENTICATE;

        final int payloadLength;
        payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
        final ByteBuf payload = cumulateBuffer.readSlice(payloadLength);
        boolean taskEnd;
        if (++this.authCounter > 100) {
            JdbdException e = new JdbdException("TooManyAuthenticationPluginNegotiations",
                    SQLStates.CONNECTION_EXCEPTION, 0);
            handleAuthenticateFailure(e);
            taskEnd = true;
        } else if (OkPacket.isOkPacket(payload)) {
            OkPacket ok = OkPacket.read(payload, this.capability);
            serverConsumer.accept(ok.getStatusFags());
            LOG.debug("MySQL authentication success,info:{}", ok.getInfo());
            this.sink.success(new AuthenticateResult(this.handshake, this.capability));
            taskEnd = true;
        } else if (ErrorPacket.isErrorPacket(payload)) {
            ErrorPacket error;
            if (this.sequenceId < 2) {
                error = ErrorPacket.read(payload, 0, obtainServerCharset());
            } else {
                error = ErrorPacket.read(payload, this.capability, obtainServerCharset());
            }
            taskEnd = true;
            handleAuthenticateFailure(MySQLExceptions.createErrorPacketException(error));
        } else {
            try {
                taskEnd = processNextAuthenticationNegotiation(payload);
            } catch (Throwable e) {
                taskEnd = true;
                handleAuthenticateFailure(MySQLExceptions.wrap(e));
            }
        }
        return taskEnd;
    }


    private int nextSequenceId() {
        int sequenceId = this.sequenceId;
        sequenceId = (++sequenceId) & 0xFF;
        this.sequenceId = sequenceId;
        return sequenceId;
    }

    private void updateSequenceId(int sequenceId) {
        this.sequenceId = sequenceId & 0xFF;
    }


    /**
     * @see #receiveHandshakeAndSendResponse(ByteBuf)
     */
    @Nullable
    private Mono<ByteBuf> createHandshakeResponsePacket() {
        Mono<ByteBuf> mono;

        try {
            final Pair<AuthenticationPlugin, Boolean> pair;
            pair = obtainAuthenticationPlugin();
            final AuthenticationPlugin plugin = pair.first;
            this.plugin = plugin;
            final ByteBuf pluginOut;
            pluginOut = createAuthenticationDataFor41(plugin, pair.second);
            final ByteBuf packet;
            packet = createHandshakeResponse41(plugin.pluginName(), pluginOut);
            LOG.debug("readableBytes:{},length:{}", packet.readableBytes(), Packets.getInt3(packet, 0));
            mono = Mono.just(packet);
        } catch (Throwable e) {
            handleAuthenticateFailure(MySQLExceptions.wrap(e));
            mono = null;
        }
        return mono;
    }

    /**
     * @see #receiveHandshakeAndSendResponse(ByteBuf)
     */
    private void sendSslRequest() {
        try {
            final Object sslObject;
            sslObject = ReactorSslProviderBuilder.builder(this)
                    .buildSslHandler();

            // add sslHandler to channel line.
            Objects.requireNonNull(this.sslConsumer, "this.sslConsumer")
                    .accept(SslWrapper.create(this, createSSlRequestPacket(), sslObject));
            if (LOG.isDebugEnabled()) {
                LOG.debug("add {} to ChannelPipeline complete.", SslHandler.class.getName());
            }
        } catch (Throwable e) {
            handleAuthenticateFailure(MySQLExceptions.wrap(e));
        }
    }

    /**
     * @see #sendSslRequest()
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html">Protocol::SSLRequest</a>
     */
    private Mono<ByteBuf> createSSlRequestPacket() {
        final ByteBuf packet;
        packet = Packets.createOnePacket(this.allocator, 32);
        // 1. client_flag
        Packets.writeInt4(packet, this.capability);
        // 2. max_packet_size
        Packets.writeInt4(packet, this.fixedEnv.maxAllowedPacket);
        // 3.handshake character_set,
        packet.writeByte(this.handshakeCollationIndex);
        // 4. filler
        packet.writeZero(23);
        Packets.writeHeader(packet, nextSequenceId());
        return Mono.just(packet);
    }


    /**
     * @return true : task end.
     * @see #authenticateDecode(ByteBuf, Consumer)
     */
    private boolean processNextAuthenticationNegotiation(final ByteBuf payload) {

        final AuthenticationPlugin authPlugin;
        if (Packets.isAuthSwitchRequestPacket(payload)) {
            payload.skipBytes(1); // skip type header
            String pluginName = Packets.readStringTerm(payload, StandardCharsets.US_ASCII);
            LOG.debug("Auth switch request method[{}]", pluginName);
            if (this.plugin.pluginName().equals(pluginName)) {
                authPlugin = this.plugin;
            } else {
                authPlugin = this.pluginMap.get(pluginName);
                if (authPlugin == null) {
                    String message = String.format("BadAuthenticationPlugin[%s] from server,please check %s %s %s three properties.",
                            pluginName, MySQLKey.DISABLED_AUTHENTICATION_PLUGINS,
                            MySQLKey.AUTHENTICATION_PLUGINS, MySQLKey.DEFAULT_AUTHENTICATION_PLUGIN);
                    handleAuthenticateFailure(new JdbdException(message));
                    return true;
                }
            }
            authPlugin.reset();
        } else {
            authPlugin = this.plugin;
            payload.skipBytes(1); // skip type header
        }
        final ByteBuf toServer;
        toServer = authPlugin.nextAuthenticationStep(payload);
        if (toServer.isReadable()) {
            this.packetPublisher = Mono.just(writeAuthPayload(toServer));
        }
        return false;
    }

    /**
     * @see #processNextAuthenticationNegotiation(ByteBuf)
     */
    private ByteBuf writeAuthPayload(final ByteBuf payload) {
        final ByteBuf packet;
        final int readableBytes = payload.readableBytes();
        if (readableBytes < Packets.MAX_PAYLOAD) {
            packet = this.adjutant.allocator().buffer(Packets.HEADER_SIZE + readableBytes);

            Packets.writeInt3(packet, readableBytes);
            packet.writeByte(nextSequenceId());
            packet.writeBytes(payload);

            payload.release(); // release payload.
        } else {
            // no bug ,never here.
            String m = String.format("%s send too long auth data.", this.plugin);
            throw new JdbdException(m);
        }
        return packet;
    }

    /**
     * @see #receiveHandshakeAndSendResponse(ByteBuf)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41">Protocol::HandshakeResponse41</a>
     */
    private ByteBuf createHandshakeResponse41(final String authPluginName, final ByteBuf pluginOut) {
        final Charset clientCharset = this.handshakeCharset;
        final int clientFlag = this.capability;

        final ByteBuf packet = Packets.createOnePacket(this.allocator, 1024);

        // 1. client_flag,Capabilities Flags, CLIENT_PROTOCOL_41 always set.
        Packets.writeInt4(packet, clientFlag);
        // 2. max_packet_size
        Packets.writeInt4(packet, this.fixedEnv.maxAllowedPacket);
        // 3. character_set
        Packets.writeInt1(packet, this.handshakeCollationIndex);
        // 4. filler,Set of bytes reserved for future use.
        packet.writeZero(23);

        // 5. username,login user name
        Packets.writeStringTerm(packet, this.host.user().getBytes(clientCharset));

        // 6. auth_response or (auth_response_length and auth_response)
        if ((clientFlag & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            Packets.writeStringLenEnc(packet, pluginOut);
        } else {
            packet.writeByte(pluginOut.readableBytes());
            packet.writeBytes(pluginOut);
        }
        pluginOut.release();

        // 7. database
        if ((clientFlag & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) {
            String database = this.host.dbName();
            if (!MySQLStrings.hasText(database)) {
                throw new JdbdException("client flag error,check this.getClientFlat() method.");
            }
            Packets.writeStringTerm(packet, database.getBytes(clientCharset));
        }
        // 8. client_plugin_name
        if ((clientFlag & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            Packets.writeStringTerm(packet, authPluginName.getBytes(clientCharset));
        }
        // 9. client connection attributes
        if ((clientFlag & Capabilities.CLIENT_CONNECT_ATTRS) != 0) {
            final Map<String, String> propertySource = createConnectionAttributes();
            // length of all key-values,affected rows
            Packets.writeIntLenEnc(packet, propertySource.size());
            for (Map.Entry<String, String> e : propertySource.entrySet()) {
                // write key
                Packets.writeStringLenEnc(packet, e.getKey().getBytes(clientCharset));
                // write value
                Packets.writeStringLenEnc(packet, e.getValue().getBytes(clientCharset));
            }

        }
        //TODO 10.zstd_compression_level,compression level for zstd compression algorithm
        //packetBuffer.writeByte(0);
        Packets.writeHeader(packet, nextSequenceId());
        return packet;
    }

    private ByteBuf createAuthenticationDataFor41(final AuthenticationPlugin plugin, final boolean skipPassword) {
        if (skipPassword) {
            return Unpooled.EMPTY_BUFFER;
        }
        final Handshake10 handshake10 = this.handshake;
        final String seed = handshake10.pluginSeed;
        final byte[] seedBytes = seed.getBytes();
        final ByteBuf fromServer;
        fromServer = this.adjutant.allocator().buffer(seedBytes.length);
        fromServer.writeBytes(seedBytes);
        // invoke AuthenticationPlugin
        final ByteBuf toServer;
        toServer = plugin.nextAuthenticationStep(fromServer);
        // release temp fromServer
        if (fromServer.refCnt() > 0) {
            fromServer.release();
        }
        if (!toServer.isReadable()) {
            String m = String.format("AuthenticationPlugin[%s] nextAuthenticationStep return error",
                    plugin.getClass().getName());
            throw new IllegalStateException(m);
        }
        return toServer;
    }

    /**
     * @see #authenticateDecode(ByteBuf, Consumer)
     * @see #onChannelClose()
     * @see #processNextAuthenticationNegotiation(ByteBuf)
     */
    private void handleAuthenticateFailure(JdbdException e) {
        this.phase = Phase.DISCONNECT;
        this.errorMessage = e.getMessage();
        this.sink.error(e);
    }


    /**
     * @see #createHandshakeResponsePacket()
     */
    private Pair<AuthenticationPlugin, Boolean> obtainAuthenticationPlugin() {
        final Map<String, AuthenticationPlugin> pluginMap = this.pluginMap;

        final Environment env = this.env;
        String pluginName = this.handshake.authPluginName;

        AuthenticationPlugin plugin;
        plugin = pluginMap.get(pluginName);
        boolean skipPassword = false;
        final boolean useSsl = isUseSsl();
        if (plugin == null) {
            plugin = pluginMap.get(env.getOrDefault(MySQLKey.DEFAULT_AUTHENTICATION_PLUGIN));
        } else if (Sha256PasswordPlugin.PLUGIN_NAME.equals(pluginName)
                && !useSsl
                && env.get(MySQLKey.SERVER_RSA_PUBLIC_KEY_FILE) == null
                && !env.getOrDefault(MySQLKey.ALLOW_PUBLIC_KEY_RETRIEVAL)) {
            /*
             * Fall back to default if plugin is 'sha256_password' but required conditions for this to work aren't met. If default is other than
             * 'sha256_password' this will result in an immediate authentication switch request, allowing for other plugins to authenticate
             * successfully. If default is 'sha256_password' then the authentication will fail as expected. In both cases user's password won't be
             * sent to avoid subjecting it to lesser security levels.
             */
            String defaultPluginName = env.getOrDefault(MySQLKey.DEFAULT_AUTHENTICATION_PLUGIN);
            skipPassword = !pluginName.equals(defaultPluginName);
            plugin = pluginMap.get(defaultPluginName);
        }
        if (plugin.requiresConfidentiality() && !useSsl) {
            String m = String.format("AuthenticationPlugin[%s] required SSL", plugin.getClass().getName());
            throw new JdbdException(m);
        }
        return Pair.create(plugin, skipPassword);
    }


    private Map<String, String> createConnectionAttributes() {
        final Map<String, String> attMap;
        attMap = MySQLStrings.spitAsMap(this.env.get(MySQLKey.CONNECTION_ATTRIBUTES), ",", ":", false);

        String clientVersion = MySQLProtocol.class.getPackage().getImplementationVersion();
        if (clientVersion == null) {
            clientVersion = "jdbd-mysql-test";
        }
        attMap.put("_client_name", "jdbd-mysql");
        attMap.put("_client_version", clientVersion);
        attMap.put("_runtime_vendor", JVM_VENDOR);
        attMap.put("_runtime_version", JVM_VERSION);
        attMap.put("_client_license", "apache");
        return attMap;
    }


    private Charset obtainServerCharset() {
        Charset charset = Charsets.getJavaCharsetByCollationIndex(this.handshake.getCollationIndex());
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }
        return charset;
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html">Capabilities Flags</a>
     */
    private int createNegotiatedCapability(final Handshake10 handshake) {
        final int serverCapability = handshake.capabilityFlags;
        final Environment env = this.env;

        final boolean useConnectWithDb;
        useConnectWithDb = MySQLStrings.hasText(this.host.dbName())
                && !env.getOrDefault(MySQLKey.CREATE_DATABASE_IF_NOT_EXIST);


        return Capabilities.CLIENT_SECURE_CONNECTION
                | Capabilities.CLIENT_PLUGIN_AUTH
                | (serverCapability & Capabilities.CLIENT_LONG_PASSWORD)  //
                | (serverCapability & Capabilities.CLIENT_PROTOCOL_41)    //

                | (serverCapability & Capabilities.CLIENT_TRANSACTIONS)   // Need this to get server status values
                | (serverCapability & Capabilities.CLIENT_MULTI_RESULTS)  // We always allow multiple result sets
                | (serverCapability & Capabilities.CLIENT_PS_MULTI_RESULTS)  // We always allow multiple result sets for SSPS
                | (serverCapability & Capabilities.CLIENT_LONG_FLAG)      //

                // | (serverCapability & Capabilities.CLIENT_DEPRECATE_EOF )  // jdbd-mysql don't deprecate eof ,because distinguish a normal resultset from an OUT parameter set.
                | (serverCapability & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
                | (env.getOrDefault(MySQLKey.USE_COMPRESSION) ? (serverCapability & Capabilities.CLIENT_COMPRESS) : 0)
                | (useConnectWithDb ? (serverCapability & Capabilities.CLIENT_CONNECT_WITH_DB) : 0)

                | (env.getOrDefault(MySQLKey.USE_AFFECTED_ROWS) ? 0 : (serverCapability & Capabilities.CLIENT_FOUND_ROWS))
                | (env.getOrDefault(MySQLKey.ALLOW_LOAD_LOCAL_INFILE) ? (serverCapability & Capabilities.CLIENT_LOCAL_FILES) : 0)
                | (env.getOrDefault(MySQLKey.INTERACTIVE_CLIENT) ? (serverCapability & Capabilities.CLIENT_INTERACTIVE) : 0)
                | (serverCapability & Capabilities.CLIENT_MULTI_STATEMENTS)

                | (env.getOrDefault(MySQLKey.DISCONNECT_ON_EXPIRED_PASSWORDS) ? 0 : (serverCapability & Capabilities.CLIENT_CAN_HANDLE_EXPIRED_PASSWORD))
                | (Constants.NONE.equals(env.get(MySQLKey.CONNECTION_ATTRIBUTES)) ? 0 : (serverCapability & Capabilities.CLIENT_CONNECT_ATTRS))
                | (env.getOrDefault(MySQLKey.SSL_MODE) != Enums.SslMode.DISABLED ? (serverCapability & Capabilities.CLIENT_SSL) : 0)
                | (serverCapability & Capabilities.CLIENT_SESSION_TRACK) // jdbd-mysql always for better sending message and receive message

                | (handshake.serverVersion.isSupportQueryAttr() ? (serverCapability & Capabilities.CLIENT_QUERY_ATTRIBUTES) : 0)
                ;
    }


    private Map<String, AuthenticationPlugin> createPluginMap() {
        final Map<String, Function<AuthenticateAssistant, AuthenticationPlugin>> pluginFuncMap;
        pluginFuncMap = this.fixedEnv.pluginFuncMap;

        final Map<String, AuthenticationPlugin> pluginMap;
        pluginMap = MySQLCollections.hashMap((int) (pluginFuncMap.size() / 0.75f));

        for (Map.Entry<String, Function<AuthenticateAssistant, AuthenticationPlugin>> e : pluginFuncMap.entrySet()) {
            pluginMap.put(e.getKey(), e.getValue().apply(this));
        }
        return MySQLCollections.unmodifiableMap(pluginMap);
    }

    /*################################## blow private static method ##################################*/


    private static Charset parseCharacterEncoding(Environment env) {
        Charset charset;
        charset = env.get(MySQLKey.CHARACTER_ENCODING);
        if (charset == null || !Charsets.isSupportCharsetClient(charset)) {
            charset = StandardCharsets.UTF_8;
        }
        return charset;

    }



    /*################################## blow private static method ##################################*/


    private enum Phase {
        RECEIVE_HANDSHAKE,
        HANDSHAKE_RESPONSE,
        AUTHENTICATE,
        DISCONNECT,
        END
    }


}
