package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.session.LocalDatabaseSession;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This class test {@link io.jdbd.session.DatabaseSession} session track
 */
@Test(dataProvider = "localSessionProvider")
public class SessionTrackTests extends SessionTestSupport {

    @Test
    public void serverTimeZoneTrack(final LocalDatabaseSession session) {
        String sql;
        // Europe/Helsinki , +00:00
        sql = "SET @@SESSION.time_zone = '+00:00', @@SESSION.character_set_results = 'utf8mb4' , @@SESSION.character_set_client = 'utf8mb4' ";

        Mono.from(session.executeUpdate(sql))
                .doOnNext(s -> LOG.info("message {}", s.message()))
                .block();

    }


    @Test
    public void validateCollation(final LocalDatabaseSession session) {
        String sql;
        sql = "show collation";

        Flux.from(session.executeQuery(sql, row -> row.getNonNull("Id", Integer.class)))
                .filter(id -> {

                    boolean match;
                    match = Charsets.INDEX_TO_COLLATION.containsKey(id);

                    LOG.info("id : {}, match : {}", id, match);
                    return match;
                })
                .blockLast();
    }


}
