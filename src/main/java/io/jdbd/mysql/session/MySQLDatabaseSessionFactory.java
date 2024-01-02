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

import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
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
import java.util.function.Function;


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

    private final String factoryName;

    private final AtomicBoolean factoryClosed = new AtomicBoolean(false);

    /**
     * <p>
     * private constructor
     * <br/>
     */
    private MySQLDatabaseSessionFactory(MySQLProtocolFactory protocolFactory, boolean forPoolVendor) {
        this.protocolFactory = protocolFactory;
        this.forPoolVendor = forPoolVendor;
        this.factoryName = this.protocolFactory.factoryName();
    }

    @Override
    public String name() {
        return this.factoryName;
    }

    @Override
    public Publisher<LocalDatabaseSession> localSession() {
        return this.localSession(null, Option.EMPTY_OPTION_FUNC);
    }

    @Override
    public Publisher<LocalDatabaseSession> localSession(final @Nullable String name, Function<Option<?>, ?> optionFunc) {
        if (this.factoryClosed.get()) {
            return Mono.error(MySQLExceptions.factoryClosed(this.factoryName));
        }
        return Mono.defer(this.protocolFactory::createProtocol)
                .map(protocol -> createLocalSession(protocol, name));
    }


    @Override
    public Publisher<RmDatabaseSession> rmSession() {
        return rmSession(null, Option.EMPTY_OPTION_FUNC);
    }

    @Override
    public Publisher<RmDatabaseSession> rmSession(final @Nullable String name, Function<Option<?>, ?> optionFunc) {
        if (this.factoryClosed.get()) {
            return Mono.error(MySQLExceptions.factoryClosed(this.factoryName));
        }
        return Mono.defer(this.protocolFactory::createProtocol)
                .map(protocol -> createRmSession(protocol, name));
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
            LOG.debug("close {}[{}] ...", className, this.factoryName);
            mono = this.protocolFactory.close()
                    .doOnSuccess(v -> LOG.debug("close {}[{}] success", className, this.factoryName))
                    .then(Mono.empty());
        } else {
            mono = Mono.empty();
        }
        return mono;
    }

    /**
     * @see #localSession()
     */
    private LocalDatabaseSession createLocalSession(final MySQLProtocol protocol, final @Nullable String name) {
        final String sessionName = name == null ? "unnamed" : name;

        final LocalDatabaseSession session;
        if (this.forPoolVendor) {
            session = MySQLLocalDatabaseSession.forPoolVendor(this, protocol, sessionName);
        } else {
            session = MySQLLocalDatabaseSession.create(this, protocol, sessionName);
        }
        return session;
    }

    /**
     * @see #rmSession()
     */
    private RmDatabaseSession createRmSession(final MySQLProtocol protocol, final @Nullable String name) {
        final String sessionName = name == null ? "unnamed" : name;
        final RmDatabaseSession session;
        if (this.forPoolVendor) {
            session = MySQLRmDatabaseSession.forPoolVendor(this, protocol, sessionName);
        } else {
            session = MySQLRmDatabaseSession.create(this, protocol, sessionName);
        }
        return session;
    }


}
