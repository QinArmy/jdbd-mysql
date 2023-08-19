package io.jdbd.mysql.session;

import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.env.MySQLHost;
import io.jdbd.mysql.env.MySQLUrlParser;
import io.jdbd.mysql.env.Protocol;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLProtocolFactory;
import io.jdbd.mysql.protocol.client.ClientProtocolFactory;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.RmDatabaseSession;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * <p>
 * This class is the implementation of {@link DatabaseSessionFactory} with MySQL protocol.
 * </p>
 *
 * @since 1.0
 */
public final class MySQLDatabaseSessionFactory implements DatabaseSessionFactory {

    public static MySQLDatabaseSessionFactory create(String url, Map<String, Object> properties)
            throws JdbdException {
        return new MySQLDatabaseSessionFactory(createProtocolFactory(url, properties), false);
    }

    public static MySQLDatabaseSessionFactory forPoolVendor(String url, Map<String, Object> properties)
            throws JdbdException {
        return new MySQLDatabaseSessionFactory(createProtocolFactory(url, properties), true);
    }


    private static MySQLProtocolFactory createProtocolFactory(String url, Map<String, Object> properties) {
        final List<MySQLHost> hostList;
        hostList = MySQLUrlParser.parse(url, properties);

        final MySQLProtocolFactory protocolFactory;
        final Protocol protocol;
        protocol = hostList.get(0).protocol();
        switch (protocol) {
            case SINGLE_CONNECTION:
                protocolFactory = ClientProtocolFactory.from(hostList.get(0));
                break;
            case FAILOVER_CONNECTION:
            case LOADBALANCE_CONNECTION:
            case REPLICATION_CONNECTION:
            case FAILOVER_DNS_SRV_CONNECTION:
            case LOADBALANCE_DNS_SRV_CONNECTION:
            case REPLICATION_DNS_SRV_CONNECTION:
                throw new JdbdException(String.format("currently don't support %s", protocol.scheme));
            default:
                throw MySQLExceptions.unexpectedEnum(protocol);
        }
        return protocolFactory;
    }


    private final MySQLProtocolFactory protocolFactory;

    private final boolean forPoolVendor;

    private final String name;

    private final AtomicBoolean closed = new AtomicBoolean(false);


    private MySQLDatabaseSessionFactory(MySQLProtocolFactory protocolFactory, boolean forPoolVendor) {
        this.protocolFactory = protocolFactory;
        this.forPoolVendor = forPoolVendor;
        this.name = this.protocolFactory.factoryName();
    }

    @Override
    public String name() {
        return this.protocolFactory.factoryName();
    }

    @Override
    public Publisher<LocalDatabaseSession> localSession() {
        if (this.closed.get()) {
            return Mono.error(MySQLExceptions.factoryClosed(this.name));
        }
        return this.protocolFactory.createProtocol()
                .map(this::createLocalSession);
    }

    @Override
    public Mono<RmDatabaseSession> rmSession() {
        if (this.closed.get()) {
            return Mono.error(MySQLExceptions.factoryClosed(this.name));
        }
        return this.protocolFactory.createProtocol()
                .map(this::createRmSession);
    }


    @Override
    public String productName() {
        return MySQLDriver.MY_SQL;
    }


    @Override
    public String factoryVendor() {
        return MySQLDriver.DRIVER_VENDOR;
    }

    @Override
    public String driverVendor() {
        return MySQLDriver.DRIVER_VENDOR;
    }

    @Override
    public DriverVersion driverVersion() {
        return MySQLDriver.getInstance().version();
    }

    @Override
    public Publisher<Void> close() {
        this.closed.set(true);
        return Mono.empty();
    }


    @Override
    public String toString() {
        return MySQLStrings.builder(50)
                .append(getClass().getName())
                .append("[ name : ")
                .append(name())
                .append(" , factoryVendor : ")
                .append(factoryVendor())
                .append(" , driverVendor : ")
                .append(driverVendor())
                .append(" , productName : ")
                .append(productName())
                .append(" , driverVersion : ")
                .append(driverVersion())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }

    /**
     * @see #localSession()
     */
    private LocalDatabaseSession createLocalSession(final MySQLProtocol protocol) {
        final LocalDatabaseSession session;
        if (this.forPoolVendor) {
            session = MySQLLocalDatabaseSession.forPoolVendor(this, protocol);
        } else {
            session = MySQLLocalDatabaseSession.create(this, protocol);
        }
        return session;
    }

    /**
     * @see #rmSession()
     */
    private RmDatabaseSession createRmSession(final MySQLProtocol protocol) {
        final RmDatabaseSession session;
        if (this.forPoolVendor) {
            session = MySQLRmDatabaseSession.forPoolVendor(this, protocol);
        } else {
            session = MySQLRmDatabaseSession.create(this, protocol);
        }
        return session;
    }


}
