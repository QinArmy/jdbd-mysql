package io.jdbd.mysql.session;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.AttributeKey;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.NettyPipeline;
import reactor.netty.tcp.SslProvider;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class ReactorNettyBugReproduceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReactorNettyBugReproduceTest.class);

    private static final int PORT = 7878;

    private static final int SSL_CAPABILITY = 1 << 11;


    @Test
    public void mysqlServer() {
        TcpServer.create()
                .host("localhost")
                .port(PORT)
                .doOnBind(c -> LOG.info("mysql public_key.pem startup "))
                .doOnConnection(this::doConnection)
                .bindUntilJavaShutdown(Duration.ofMinutes(15), c -> {
                });
    }

    @Test
    public void test() throws Exception {
        TcpClient.create()
                .host("localhost")
                .port(PORT)
                .connect()
                .map(this::executeProtocol)
                .flatMap(Connection::onTerminate)
                .block();
    }

    private void doConnection(Connection connection) {
        Channel channel = connection.channel();
        ChannelId channelId = channel.id();
        AttributeKey<ClientHandler> clientAttributeKey = AttributeKey.valueOf("$" + channelId);

        if (channel.hasAttr(clientAttributeKey)) {
            // here also is Reactor netty bug,because just add SslHandler,not new Connection.
            LOG.info("ssl handshake  success.");
            // I have to creat new ClientHandler for receive Handshake response from client.
            // because Reactor Netty bug(Reactor Netty suppose when ssl handshake success that is new connect ,actually thant is old connection(just ssl handshake success). )
            ClientHandler clientHandler = new ClientHandler(connection);
            clientHandler.phase = ClientHandler.Phase.AUTH_RESULT;
            connection.inbound().receive().subscribe(clientHandler);

            // channel.attr(clientAttributeKey).set(clientHandler);
        } else {
            LOG.info("receive connect from client,id:{}", channelId);

            ClientHandler clientHandler = new ClientHandler(connection);
            connection.channel()
                    .attr(clientAttributeKey).set(clientHandler);

            connection.inbound().receive()
                    .subscribe(clientHandler);
            // send handshake to client.
            ByteBuf packet = connection.channel().alloc().buffer(4);
            packet.writeInt(SSL_CAPABILITY);
            Flux.from(connection.outbound().send(Mono.just(packet)))
                    .doOnComplete(() -> {
                        LOG.info("write handshake to client success");
                    })
                    .subscribe();

        }


    }


    private Connection executeProtocol(Connection connection) {
        LOG.info("client connect success,{}", connection.channel().id());
        connection.inbound()
                .receive()
                .subscribe(new ClientSubscriber(connection));
        return connection;
    }


    private static SslProvider createSslProvider(boolean client) throws Exception {
        List<String> cipherList = Arrays.asList("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
                , "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
                , "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");

        SslContextBuilder builder;
        if (client) {
            builder = SslContextBuilder.forClient();
        } else {
            builder = SslContextBuilder.forServer(getPrivateKey(), getCertificate())
            ;

        }
        SslContext sslContext = builder.trustManager(new SimpleX509TrustManager())
                .ciphers(cipherList)
                .protocols("TLSv1", "TLSv1.1", "TLSv1.2")
                .build();

        return SslProvider.builder()
                .sslContext(sslContext)
                .build();
    }

    private static PrivateKey getPrivateKey() throws Exception {
        String base64 = "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCp8WBYM+BTXeTX\n" +
                "AFemmP1GalfEzz1SDQaQScRDEixEh3ohMPQ33Zy/YPEJGfQsBirgviDpsLib90QA\n" +
                "yfn84621414KQAjBacF1OMlbxqHFG3V4IqJyTQrirLNdrwYS4dMKhsD7gomIQ5VO\n" +
                "3tdpzUHG2D3ps9UXSAyI7JOKt5OhblOSXqgpUF3E4zw55oUY3jh0lvIHsogifU6V\n" +
                "CBD3CXAnX237w2QVvRnxDlZZTXfD6QZCorYWEi7FVA60uiPTWbUXCe9y6506uBs5\n" +
                "Jqn9A+KK/Rxf9o3A+J2UQnuRKxKQm9uThXfo9jawrSUAgPhE+U72w0bZt0lJ4Sru\n" +
                "op1qGiONAgMBAAECggEBAI9N29CE7kVeh/pFb6QsnmcihaCQoUTvdvl1OurUCEBB\n" +
                "fds/TLFPTz6SoK7sE9qL2Nxrd5WYUBQ1DkMcDpIR7AnVERfTp45xf5E5sZKSjReJ\n" +
                "cCU+D34TOXqr5xS44oZJp4zY1SNBkHg3hVBc2Yl+bFkhQBQycR6QwwEzfbcb1oHp\n" +
                "BtB2efyy0ieiNzDVYg2Vqoln5ccMm+pNmpuo6KO7LjKo4zhk1IAF0KCsJqU9AmPR\n" +
                "kgPBUwqpQGAYd0P+UMmz7q1QwY+KzXBnYzZ6hUoS8zbE4iXMqGOemUYFIj9SN55K\n" +
                "hitHXcMHGoLhsYJY+n3vGbLNj9yJbmOWSE9u5nRSNAECgYEA2CkXnT2V3NG9t2qv\n" +
                "818Aq1BMGMlqcD2P4HH7Bp4hr7gMxbuaM60M0aG4cdeD1kWXOpsoGnwz0u05vdUa\n" +
                "AVUisRDIznKvUgJ++1SKRmFfzHJrSv0lAlVuwtLJizIciJ6EEFgEWYdsLck/b/xI\n" +
                "lNO5Cn4Od9qjchwGPCeWLGgAGfkCgYEAyUOjfgJ/SiEBNPipEQuUqB/of8n7JWz2\n" +
                "TS+icLfn0R8FeWcb78GnwYkDcM3Qj/98rqZaCfrUrCTF3NKhE3v4Mygz3f9v5Kn4\n" +
                "Z1oFzZLuXTQHjGhiAciY1y0+8PGQuu9EPk8oIu2ZBI8ZRHz7epvl4i8AGVzwzoVw\n" +
                "mAjBx0PzmzUCgYEAvB5O8cYmBS33qIdNp0S3pV/VSgEIA7Rf4Vnwt9qowG8xlmfl\n" +
                "FDH3JP1UMqb3kmOv8A4Vwa7zvw47IS0zW5OHBIfx6lE+qOWJMxto10VpBNlS7MkQ\n" +
                "C07kfOLLCbvLv04M6theuLe1esdY0RHC7NqxgCMiXkZF1knzVyiwdebwp6kCgYAs\n" +
                "tro75JIjBfIesp/dCZWdRHmC6nzBc9PEkjCkmjcGXr34ms+6FGwQsz/wb8lGNJye\n" +
                "sJaQYQmetQzAYosmqQQbWXMsfvN8+cYWMAnaAiqyyjxjFU2w18AdDhBNt11QKpge\n" +
                "v7dLCz5TpPcYICw+sSQBfC+pctyNBVhebpOekZotgQKBgCTjwPpmUbrewUTTZNIB\n" +
                "/4O5ZE0UGnqWNH2ugJCQTJlK4gutJPsHrSHhui2VBtKE6bWXCr3daT9XE5vw/Roi\n" +
                "hgwBsk9zLMZ9Z/E4He1KEQKofDHD1KEQUSuQkNa7/+mg3n+4+liEOiQx2Sgw/Qs6\n" +
                "D69aG+wk/c5/zl2p3CQbT63F";

        Key aesKey = new SecretKeySpec(Base64.getMimeDecoder().decode("52C332763B19BEA127620A1AF3C56A38"), "AES");
//        Cipher cipher = Cipher.getInstance("AES");
//        cipher.init(Cipher.DECRYPT_MODE, aesKey);
//        byte[] keyEncodedBytes =   cipher.doFinal( Base64.getMimeDecoder().decode(base64));
//        System.out.println(new String(keyEncodedBytes));

        KeyFactory keyFactory = KeyFactory.getInstance("RSA");

        byte[] keyEncodedBytes = Base64.getMimeDecoder().decode(base64);
        return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(keyEncodedBytes));
    }

    private static X509Certificate getCertificate() throws Exception {
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        String rootCsr = "-----BEGIN CERTIFICATE-----\n" +
                "MIIDbjCCAlYCCQDw+6Tnxmab8DANBgkqhkiG9w0BAQUFADB5MQswCQYDVQQGEwJD\n" +
                "TjERMA8GA1UECAwIemhlamlhbmcxETAPBgNVBAcMCGhhbmd6aG91MRAwDgYDVQQK\n" +
                "DAdxaW5hcm15MRIwEAYDVQQDDAlsb2NhbGhvc3QxHjAcBgkqhkiG9w0BCQEWD3pv\n" +
                "cm9AcWluYXJteS5pbzAeFw0yMTAzMTAxMDQ3MDNaFw0yMjAzMTAxMDQ3MDNaMHkx\n" +
                "CzAJBgNVBAYTAkNOMREwDwYDVQQIDAh6aGVqaWFuZzERMA8GA1UEBwwIaGFuZ3po\n" +
                "b3UxEDAOBgNVBAoMB3FpbmFybXkxEjAQBgNVBAMMCWxvY2FsaG9zdDEeMBwGCSqG\n" +
                "SIb3DQEJARYPem9yb0BxaW5hcm15LmlvMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A\n" +
                "MIIBCgKCAQEAqfFgWDPgU13k1wBXppj9RmpXxM89Ug0GkEnEQxIsRId6ITD0N92c\n" +
                "v2DxCRn0LAYq4L4g6bC4m/dEAMn5/OOtteNeCkAIwWnBdTjJW8ahxRt1eCKick0K\n" +
                "4qyzXa8GEuHTCobA+4KJiEOVTt7Xac1Bxtg96bPVF0gMiOyTireToW5Tkl6oKVBd\n" +
                "xOM8OeaFGN44dJbyB7KIIn1OlQgQ9wlwJ19t+8NkFb0Z8Q5WWU13w+kGQqK2FhIu\n" +
                "xVQOtLoj01m1FwnvcuudOrgbOSap/QPiiv0cX/aNwPidlEJ7kSsSkJvbk4V36PY2\n" +
                "sK0lAID4RPlO9sNG2bdJSeEq7qKdahojjQIDAQABMA0GCSqGSIb3DQEBBQUAA4IB\n" +
                "AQB54ULUrSpmeLGxppHbeMcuIEfRurQUs/cf5TGasMEPdsDJldw5xUG+E44fh2B4\n" +
                "Ad2fkrJz64IgvanpVISpPRYebxMY5r0pr1th5/VIiapUrCa4GhhyruV1oVhzZBN9\n" +
                "TIMp95pyysX+a3ijSTiV4tgjOrW9r+FArZq62AOfw/rCe00HYfjTVl1uxZnvS3Sm\n" +
                "PADsK3ELPO1MX8F9wR+0nY2AAOAE1U2gPyHTd8EESf7rJzNE1hY/6Nl9WQS9MvyN\n" +
                "aRNw2iiU13XOICFXRAs0kuT9uxJIbU1qXZs5fA8VexdGcYJVd4KmnIqUiM04WzrE\n" +
                "5Ksf8NqtM0nT/2bYn9yQw+8x\n" +
                "-----END CERTIFICATE-----\n";


//         Path path = Paths.get("/Users/zoro/repository/my-github/idea/jdbd/fd.crt");
//         try(InputStream in = Files.newInputStream(path)){
//             return (X509Certificate)certificateFactory.generateCertificate(in);
//         }

        return (X509Certificate) certificateFactory.generateCertificate(new ByteArrayInputStream(rootCsr.getBytes()));

    }


    private static final class ClientSubscriber implements CoreSubscriber<ByteBuf> {

        enum Phase {
            RECEIVE_HANDSHAKE,
            SSL_REQUEST,
            HANDSHAKE_RESPONSE,
        }


        private final Connection connection;

        private Phase phase = Phase.RECEIVE_HANDSHAKE;

        private int capabilities = 0;


        private ClientSubscriber(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuf byteBuf) {
            switch (this.phase) {
                case RECEIVE_HANDSHAKE: {
                    receiveHandshake(byteBuf);
                }
                break;
                default: {
                    // never here ,because reactor.netty.tcp.SslProvider.SslReadHandler.userEventTriggered bug.
                    LOG.info("receive data from mysql public_key.pem.");
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            LOG.error("channel error", t);
        }

        @Override
        public void onComplete() {
            LOG.info("channel close.");
        }


        private void receiveHandshake(ByteBuf byteBuf) {
            this.capabilities = byteBuf.readInt();
            if ((this.capabilities & SSL_CAPABILITY) != 0) {
                this.phase = Phase.SSL_REQUEST;
                //send ssl request to mysql public_key.pem.
                ByteBuf packet = this.connection.channel().alloc().buffer(4);
                packet.writeInt(SSL_CAPABILITY);
                Flux.from(this.connection.outbound().send(Mono.just(packet)))
                        .doOnComplete(this::addSsl)
                        .subscribe();

            }
        }


        private void addSsl() {
            LOG.info("send ssl request to public_key.pem success");
            LOG.info("add SslHandler");
            try {
                Thread.sleep(2 * 1000L); //wait for public_key.pem add SslHandler.

                ChannelPipeline pipeline = this.connection.channel().pipeline();
                pipeline.addBefore(NettyPipeline.ReactiveBridge, NettyPipeline.LoggingHandler, new ChannelInboundHandlerAdapter() {


                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        super.channelActive(ctx);
                    }

                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                        super.userEventTriggered(ctx, evt);
                        if (evt instanceof SslHandshakeCompletionEvent && ((SslHandshakeCompletionEvent) (evt)).isSuccess()) {
                            LOG.info("ssl handshake success");

                            ByteBuf byteBuf = ClientSubscriber.this.connection.channel().alloc().buffer(100);
                            byteBuf.writeBytes("my password".getBytes(StandardCharsets.UTF_8));
                            // send handshake response to public_key.pem.
                            ClientSubscriber.this.phase = Phase.HANDSHAKE_RESPONSE;
                            Flux.from(ClientSubscriber.this.connection.outbound().send(Mono.just(byteBuf)))
                                    .doOnComplete(() -> LOG.info("send handshake response to public_key.pem success "))
                                    .subscribe();
                        }
                    }


                });

                SslProvider sslProvider = createSslProvider(true);
                InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", PORT);
                sslProvider.addSslHandler(this.connection.channel(), address, false);


            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }


    }


    private static final class SimpleX509TrustManager implements X509TrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            for (X509Certificate x509Certificate : x509Certificates) {
                x509Certificate.checkValidity();
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            for (X509Certificate x509Certificate : x509Certificates) {
                x509Certificate.checkValidity();
            }
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }


    private static final class ClientHandler implements CoreSubscriber<ByteBuf> {

        enum Phase {
            SSL_REQUEST,
            AUTH_RESULT,
            OTHER
        }

        private final Connection connection;

        private Phase phase = Phase.SSL_REQUEST;

        private ClientHandler(Connection connection) {
            this.connection = connection;
        }


        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuf byteBuf) {
            switch (this.phase) {
                case SSL_REQUEST: {
                    LOG.info("receive ssl request from client.");
                    InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", PORT);
                    try {
                        createSslProvider(false)
                                .addSslHandler(this.connection.channel(), address, false);
                        LOG.info("public_key.pem add SslHandler");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                break;
                case AUTH_RESULT: {
                    LOG.info("receive handshake response from client.");
                    ByteBuf packet = this.connection.channel().alloc().buffer(100);
                    packet.writeBytes("authenticate success".getBytes(StandardCharsets.UTF_8));
                    Flux.from(this.connection.outbound().send(Mono.just(packet)))
                            .doOnComplete(() -> {
                                LOG.info("send authenticate success to client success.");
                                this.phase = Phase.OTHER;
                            })
                            .subscribe()
                    ;
                }
                break;
                default: {
                    // here ,Reactor netty no bug.
                    LOG.info("receive other packet from client");
                }
            }

        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onComplete() {

        }


    }


}
