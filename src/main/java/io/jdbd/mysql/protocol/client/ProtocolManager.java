package io.jdbd.mysql.protocol.client;

import reactor.core.publisher.Mono;

interface ProtocolManager {

    TaskAdjutant adjutant();

    Mono<Void> reset();

    Mono<Void> reConnect();

}
