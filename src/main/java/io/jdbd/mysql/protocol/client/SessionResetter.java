package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.SessionEnv;
import reactor.core.publisher.Mono;

interface SessionResetter {

    Mono<SessionEnv> reset();
}
