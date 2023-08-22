package io.jdbd.mysql.protocol.client;

import reactor.core.publisher.Mono;

import java.time.Duration;

interface ProtocolManager {

    TaskAdjutant adjutant();

    Mono<Void> reset();

    Mono<Void> reConnect(Duration duration);

}
