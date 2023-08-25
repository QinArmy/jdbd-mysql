package io.jdbd.mysql.protocol;

import io.jdbd.session.OptionSpec;
import reactor.core.publisher.Mono;

public interface MySQLProtocolFactory extends OptionSpec {

    String factoryName();

    Mono<MySQLProtocol> createProtocol();


    /**
     * override {@link Object#toString()}
     *
     * @return driver info, contain : <ol>
     * <li>implementation class name</li>
     * <li>{@link #factoryName()}</li>
     * <li>{@link System#identityHashCode(Object)}</li>
     * </ol>
     */
    @Override
    String toString();


}
