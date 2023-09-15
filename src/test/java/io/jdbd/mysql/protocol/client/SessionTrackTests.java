package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.ClientTestUtils;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.session.SessionTestSupport;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.result.ResultRow;
import io.jdbd.session.LocalDatabaseSession;
import org.testng.Assert;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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


    /**
     * just for generate correct {@link Collation} code
     */
    @Test(enabled = false)
    public void validateCollation(final LocalDatabaseSession session) throws Exception {
        String sql;
        sql = "select co.ID, co.COLLATION_NAME, co.CHARACTER_SET_NAME\n" +
                "from information_schema.CHARACTER_SETS as ch\n" +
                "         join information_schema.COLLATIONS as co on ch.CHARACTER_SET_NAME = co.CHARACTER_SET_NAME\n" +
                "order by co.ID";
        final List<ResultRow> rowList;
        rowList = Flux.from(session.executeQuery(sql))
                .collectList()
                .block();

        Assert.assertNotNull(rowList);

        final Map<Integer, Collation> collationMap = MySQLCollections.hashMap();
        Collation collation;
        int id;
        for (ResultRow row : rowList) {
            id = row.getNonNull("ID", Integer.class);
            collation = Charsets.INDEX_TO_COLLATION.get(id);
            if (collation == null) {
                // LOG.info("unknown collation : {} ", id);
                continue;
            }
            collationMap.put(id, new Collation(id, row.getNonNull("COLLATION_NAME", String.class), collation.priority, row.getNonNull("CHARACTER_SET_NAME", String.class)));
        }

        final Set<Integer> idSet;
        idSet = MySQLCollections.hashSet(Charsets.INDEX_TO_COLLATION.keySet());

        idSet.removeAll(collationMap.keySet());

        for (Integer index : idSet) {
            collationMap.putIfAbsent(index, Charsets.INDEX_TO_COLLATION.get(index));
        }

        final List<Collation> collationList = MySQLCollections.arrayList(collationMap.values());

        collationList.sort(Comparator.comparingInt(Collation::index));

        Path path = Paths.get(ClientTestUtils.getTestResourcesPath().toString(), "my-local/myLocal.txt");
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            final StringBuilder builder = new StringBuilder(128);
            final ByteBuffer buffer = ByteBuffer.allocate(2048);

            final int collationSize = collationList.size();
            LOG.info("collationSize : {}", collationSize);
            for (int i = 0; i < collationSize; i++) {
                collation = collationList.get(i);

                if (i > 0) {
                    builder.append(System.lineSeparator());
                    if ((i & 3) == 0) {
                        builder.append(System.lineSeparator());
                    }
                }

                builder.append("list.add(new Collation(")
                        .append(collation.index)
                        .append(Constants.COMMA)
                        .append(Constants.DOUBLE_QUOTE)
                        .append(collation.name)
                        .append(Constants.DOUBLE_QUOTE)
                        .append(Constants.COMMA)
                        .append(collation.priority)
                        .append(Constants.COMMA)
                        .append(Constants.DOUBLE_QUOTE)
                        .append(collation.charsetName)
                        .append(Constants.DOUBLE_QUOTE)
                        .append("));");

                if (idSet.contains(collation.index)) {
                    builder.append(" // ")
                            .append(collation.index)
                            .append(" don't exists in database, MySQL 8.1.0 mac");
                }

                buffer.put(builder.toString().getBytes(StandardCharsets.UTF_8));
                buffer.flip();
                channel.write(buffer);
                buffer.clear();

                builder.setLength(0);

            }


//             if(!collation.name.equalsIgnoreCase(row.getNonNull("Collation",String.class))){
//                    LOG.error("error collation id : {} ,  name : {} correct name : {}, ",id ,collation.name,row.getNonNull("Collation",String.class));
//             }
//                if (!collation.myCharset.name.equalsIgnoreCase(row.getNonNull("Charset", String.class))) {
//                    LOG.error("error collation id : {},  name : {}, charset name : {},correct name : {}", collation.index, collation.name, collation.myCharset.name, row.getNonNull("Charset", String.class));
//                }


        }// try


    }

    @Test
    public void validateCharset(final LocalDatabaseSession session) {
        String sql;
        sql = "show charset";
        final Map<String, Integer> map;
        map = Flux.from(session.executeQuery(sql))
                .collectMap(this::charsetRowKey, this::charsetRowValue, MySQLCollections::hashMap)
                .block();

        Assert.assertNotNull(map);

        MyCharset charset;
        for (Map.Entry<String, Integer> e : map.entrySet()) {
            charset = Charsets.NAME_TO_CHARSET.get(e.getKey());
            if (charset == null) {
                LOG.info("unknown charset : {} , Maxlen : {}", e.getKey(), e.getValue());
                continue;
            }
            if (charset.mblen != e.getValue()) {
                LOG.error("error charset : {} , Maxlen : {} , correct value  {} ", charset.name, charset.mblen, e.getValue());
            }
        }
    }

    private String charsetRowKey(ResultRow row) {
        return row.getNonNull("Charset", String.class);
    }

    private Integer charsetRowValue(ResultRow row) {
        return row.getNonNull("Maxlen", Integer.class);
    }


}
