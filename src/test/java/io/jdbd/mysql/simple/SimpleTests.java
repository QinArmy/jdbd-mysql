package io.jdbd.mysql.simple;

import io.jdbd.Driver;
import io.jdbd.meta.JdbdType;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public class SimpleTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTests.class);

    private static DatabaseSessionFactory sessionFactory;

    @BeforeClass
    public void createSessionFactory() {
        if (sessionFactory == null) {
            return;
        }
        String url;
        final Map<String, Object> map = new HashMap<>();

        url = "jdbd:mysql://localhost:3306/army_test?factoryWorkerCount=30";

        map.put(Driver.USER, "army_w");
        map.put(Driver.PASSWORD, "army123");

        sessionFactory = Driver.findDriver(url).forDeveloper(url, map);
    }

    @Test
    public void insertStatement() {
        final ResultStates resultStates;
        resultStates = Mono.from(sessionFactory.localSession())
                .flatMap(this::bindAndInsert)
                .block();

        Assert.assertNotNull(resultStates);
        Assert.assertEquals(resultStates.affectedRows(), 1);
        LOG.info("auto generate id : {}", resultStates.lastInsertedId());
    }

    private Mono<ResultStates> bindAndInsert(final LocalDatabaseSession localSession) {
        final String sql;
        sql = "INSERT mysql_types(my_boolean,my_bigint,my_datetime,my_datetime6,my_var_char200,) VALUES (?,?,?,?,?)";
        Mono.from(localSession.startTransaction())
                .flatMap(session -> session.bindStatement(sql)
                        .bind(0, JdbdType.BOOLEAN, Boolean.TRUE)
                        .bind(1, JdbdType.BIGINT, null)
                        .bind(2, JdbdType.TIMESTAMP, LocalDateTime.now())
                        .bind(3, JdbdType.TIMESTAMP_WITH_TIMEZONE, OffsetDateTime.now(ZoneOffset.UTC))
                        .bind(4, JdbdType.VARCHAR, "中国 QinArmy's jdbd \n \\ \t \" \032 \b \r '''  \\' ")
                        .executeUpdate(Mono::from)
                );

        return Mono.empty();
    }


}
