package io.jdbd.mysql.env;

import io.jdbd.JdbdException;
import reactor.util.annotation.Nullable;

/**
 * <p>
 * see {@code com.mysql.cj.conf.ConnectionUrl.Type}
 * </p>
 */
public enum Protocol {

    // DNS SRV schemes (cardinality is validated by implementing classes):
    FAILOVER_DNS_SRV_CONNECTION("jdbd:mysql+srv:", HostsCardinality.ONE_OR_MORE), //
    LOADBALANCE_DNS_SRV_CONNECTION("jdbd:mysql+srv:loadbalance:", HostsCardinality.ONE_OR_MORE), //
    REPLICATION_DNS_SRV_CONNECTION("jdbd:mysql+srv:replication:", HostsCardinality.ONE_OR_MORE), //
    // Standard schemes:
    SINGLE_CONNECTION("jdbd:mysql:", HostsCardinality.SINGLE, FAILOVER_DNS_SRV_CONNECTION), //
    FAILOVER_CONNECTION("jdbd:mysql:", HostsCardinality.MULTIPLE, FAILOVER_DNS_SRV_CONNECTION), //
    LOADBALANCE_CONNECTION("jdbd:mysql:loadbalance:", HostsCardinality.ONE_OR_MORE, LOADBALANCE_DNS_SRV_CONNECTION), //
    REPLICATION_CONNECTION("jdbd:mysql:replication:", HostsCardinality.ONE_OR_MORE, REPLICATION_DNS_SRV_CONNECTION); //

    public final String scheme;
    public final HostsCardinality cardinality;
    public final Protocol alternateDnsSrvType;

    Protocol(String scheme, HostsCardinality cardinality) {
        this(scheme, cardinality, null);
    }

    Protocol(String scheme, HostsCardinality cardinality, @Nullable Protocol alternateDnsSrvType) {
        this.scheme = scheme;
        this.cardinality = cardinality;
        this.alternateDnsSrvType = alternateDnsSrvType;
    }


    /**
     * Returns the {@link Protocol} corresponding to the given scheme and number of hosts, if any.
     * Otherwise throws an {@link JdbdException}.
     * Calling this method with the argument n lower than 0 skips the hosts cardinality validation.
     *
     * @param scheme one of supported schemes
     * @param n      the number of hosts in the database URL
     * @return the {@link Protocol} corresponding to the given protocol and number of hosts
     */
    public static Protocol fromValue(String url, String scheme, int n) {
        for (Protocol t : Protocol.values()) {
            if (t.scheme.equalsIgnoreCase(scheme) && (n < 0 || t.cardinality.assertSize(n))) {
                return t;
            }
        }
        String m = String.format("unsupported scheme[%s] and hosts cardinality[%s] in url[%s]", scheme, n, url);
        throw new JdbdException(m);
    }

    /**
     * Checks if the given scheme corresponds to one of the connection types the driver supports.
     *
     * @param scheme scheme part from connection string, like "jdbd:mysql:"
     * @return true if the given scheme is supported by driver
     */
    public static boolean isSupported(String scheme) {
        boolean support = false;
        for (Protocol t : values()) {
            if (t.scheme.equalsIgnoreCase(scheme)) {
                support = true;
                break;
            }
        }
        return support;
    }


}
