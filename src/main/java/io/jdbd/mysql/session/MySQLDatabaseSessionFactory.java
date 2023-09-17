package io.jdbd.mysql.session;

import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.env.MySQLHostInfo;
import io.jdbd.mysql.env.MySQLUrlParser;
import io.jdbd.mysql.env.Protocol;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLProtocolFactory;
import io.jdbd.mysql.protocol.client.ClientProtocolFactory;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.session.RmDatabaseSession;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * <p>
 * This class is the implementation of {@link DatabaseSessionFactory} with MySQL protocol.
 * <br/>
 *
 * @since 1.0
 */
public final class MySQLDatabaseSessionFactory implements DatabaseSessionFactory {

    public static MySQLDatabaseSessionFactory create(String url, Map<String, Object> properties, boolean forPoolVendor)
            throws JdbdException {
        return new MySQLDatabaseSessionFactory(createProtocolFactory(url, properties, forPoolVendor), forPoolVendor);
    }


    private static MySQLProtocolFactory createProtocolFactory(String url, Map<String, Object> properties,
                                                              final boolean forPoolVendor) {
        final List<MySQLHostInfo> hostList;
        hostList = MySQLUrlParser.parse(url, properties);

        final MySQLProtocolFactory protocolFactory;
        final Protocol protocol;
        protocol = hostList.get(0).protocol();
        switch (protocol) {
            case SINGLE_CONNECTION:
                protocolFactory = ClientProtocolFactory.from(hostList.get(0), forPoolVendor);
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


    private static final Logger LOG = LoggerFactory.getLogger(MySQLDatabaseSessionFactory.class);

    private final MySQLProtocolFactory protocolFactory;

    private final boolean forPoolVendor;

    private final String name;

    private final AtomicBoolean factoryClosed = new AtomicBoolean(false);

    /**
     * <p>
     * private constructor
     * <br/>
     */
    private MySQLDatabaseSessionFactory(MySQLProtocolFactory protocolFactory, boolean forPoolVendor) {
        this.protocolFactory = protocolFactory;
        this.forPoolVendor = forPoolVendor;
        this.name = this.protocolFactory.factoryName();
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public Publisher<LocalDatabaseSession> localSession() {
        if (this.factoryClosed.get()) {
            return Mono.error(MySQLExceptions.factoryClosed(this.name));
        }
        return Mono.defer(this.protocolFactory::createProtocol)
                .map(this::createLocalSession);
    }


    @Override
    public Mono<RmDatabaseSession> rmSession() {
        if (this.factoryClosed.get()) {
            return Mono.error(MySQLExceptions.factoryClosed(this.name));
        }
        return Mono.defer(this.protocolFactory::createProtocol)
                .map(this::createRmSession);
    }


    @Override
    public String productFamily() {
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
    public <T> Publisher<T> close() {
        return Mono.defer(this::closeFactory);
    }

    @Override
    public boolean isClosed() {
        return this.factoryClosed.get();
    }


    @Override
    public <T> T valueOf(Option<T> option) {
        return this.protocolFactory.valueOf(option);
    }

    @Override
    public String toString() {
        return MySQLStrings.builder(210)
                .append(getClass().getName())
                .append("[ name : ")
                .append(name())
                .append(" , factoryVendor : ")
                .append(factoryVendor())
                .append(" , driverVendor : ")
                .append(driverVendor())
                .append(" , productFamily : ")
                .append(productFamily())
                .append(" , driverVersion : ")
                .append(driverVersion().getVersion())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }

    /**
     * @see #close()
     */
    private <T> Mono<T> closeFactory() {
        final Mono<T> mono;
        if (this.factoryClosed.compareAndSet(false, true)) {
            final String className = getClass().getName();
            LOG.debug("close {}[{}] ...", className, this.name);
            mono = this.protocolFactory.close()
                    .doOnSuccess(v -> LOG.debug("close {}[{}] success", className, this.name))
                    .then(Mono.empty());
        } else {
            mono = Mono.empty();
        }
        return mono;
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
