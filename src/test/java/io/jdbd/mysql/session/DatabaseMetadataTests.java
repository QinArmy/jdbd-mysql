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

import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.meta.SchemaMeta;
import io.jdbd.meta.TableMeta;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.vendor.meta.VendorSchemaMeta;
import io.jdbd.vendor.meta.VendorTableColumnMeta;
import io.jdbd.vendor.meta.VendorTableIndexMeta;
import io.jdbd.vendor.meta.VendorTableMeta;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Function;

/**
 * <p>
 * This class is the test class of {@link MySQLDatabaseMetadata}
 * <br/>
 */
@Test(dataProvider = "databaseMetadataProvider")
public class DatabaseMetadataTests extends SessionTestSupport {


    /**
     * @see DatabaseMetaData#currentSchema(Function)
     */
    @Test
    public void currentSchema(final DatabaseMetaData metaData) {
        final SchemaMeta schemaMeta;
        schemaMeta = Mono.from(metaData.currentSchema(Option.EMPTY_OPTION_FUNC))
                .block();
        Assert.assertTrue(schemaMeta instanceof VendorSchemaMeta);
        LOG.info("currentSchema test result {}", schemaMeta);
    }

    /**
     * @see DatabaseMetaData#schemas(Function)
     */
    @Test
    public void schemas(final DatabaseMetaData metaData) {
        Flux.from(metaData.schemas(Option.EMPTY_OPTION_FUNC))
                .doOnNext(s -> Assert.assertTrue(s instanceof VendorSchemaMeta))
                .doOnNext(s -> LOG.info("schema : {}", s.toString())) // use toString() ,test bug
                .count()
                .doOnNext(s -> LOG.info("schema count : {}", s))
                .block();
    }

    /**
     * @see DatabaseMetaData#tablesOfCurrentSchema(Function)
     */
    @Test
    public void tablesOfCurrentSchema(final DatabaseMetaData metaData) {

        Flux.from(metaData.tablesOfCurrentSchema(Option.EMPTY_OPTION_FUNC))
                .doOnNext(s -> Assert.assertTrue(s instanceof VendorTableMeta))
                .doOnNext(s -> LOG.info("tablesOfCurrentSchema item : {}", s.toString())) // use toString() ,test bug
                .count()
                .doOnNext(c -> LOG.info("tablesOfCurrentSchema table count : {}", c))
                .block();

    }

    /**
     * @see DatabaseMetaData#tablesOfSchema(SchemaMeta, Function)
     */
    @Test(invocationCount = 10, dataProvider = "tablesOfSchemaProvider", dependsOnMethods = "schemas")
    public void tablesOfSchema(final DatabaseMetaData metaData, final Function<Option<?>, ?> optionFunc) {

        final Long count;
        count = Flux.from(metaData.schemas(Option.EMPTY_OPTION_FUNC))
                .flatMap(s -> s.databaseMetadata().tablesOfSchema(s, optionFunc))
                .doOnNext(s -> Assert.assertTrue(s instanceof VendorTableMeta))
                .doOnNext(s -> LOG.info("tablesOfSchema item : {}", s.toString())) // use toString() ,test bug
                .doOnNext(s -> LOG.info("tablesOfSchema privilegeSet : {}", s.privilegeSet()))
                .count()
                .doOnNext(c -> LOG.info("tablesOfSchema table count : {}", c))
                .block();

        Assert.assertNotNull(count);

    }

    /**
     * @see DatabaseMetaData#columnsOfTable(TableMeta, Function)
     */
    @Test(invocationCount = 4, dataProvider = "columnsOfTableProvider", dependsOnMethods = {"schemas", "tablesOfCurrentSchema"})
    public void columnsOfTable(final DatabaseMetaData metaData, final Function<Option<?>, ?> optionFunc) {

        Flux.from(metaData.tablesOfCurrentSchema(Option.EMPTY_OPTION_FUNC))
                .flatMap(s -> metaData.columnsOfTable(s, optionFunc))
                .doOnNext(s -> Assert.assertTrue(s instanceof VendorTableColumnMeta))
                .doOnNext(s -> LOG.info("columnsOfTable item : {}", s.toString())) // use toString() ,test bug
                .doOnNext(s -> LOG.info("columnsOfTable privilegeSet : {}", s.privilegeSet()))
                .filter(s -> s.dataType() != MySQLType.UNKNOWN)
                .count()
                .doOnNext(n -> LOG.info("columnsOfTable table count : {}", n))
                .doOnNext(n -> Assert.assertTrue(n > 0))
                .block();
    }

    /**
     * @see DatabaseMetaData#indexesOfTable(TableMeta, Function)
     */
    @Test(dependsOnMethods = {"schemas", "tablesOfCurrentSchema"})
    public void indexesOfTable(final DatabaseMetaData metaData) {
        final Map<Option<?>, Object> optionMap = MySQLCollections.hashMap();

        // optionMap.put(Option.NAME,"mysql_types_pk_3434,PRIMARY,mysql_types_id_index34567");
        // optionMap.put(Option.UNIQUE,Boolean.FALSE);
        // optionMap.put(VendorOptions.INDEX_TYPE,"%");
        Flux.from(metaData.tablesOfCurrentSchema(Option.EMPTY_OPTION_FUNC))
                .flatMap(s -> metaData.indexesOfTable(s, optionMap::get))
                .doOnNext(s -> Assert.assertTrue(s instanceof VendorTableIndexMeta))
                .doOnNext(s -> LOG.info("indexesOfTable item : {}", s.toString())) // use toString() ,test bug
                .blockLast();
    }

    /**
     * @see DatabaseMetaData#sqlKeyWords(boolean)
     */
    @Test
    public void sqlKeyWords(final DatabaseMetaData metaData) {

        Mono.from(metaData.sqlKeyWords(true))
                .doOnNext(map -> Assert.assertTrue(map.size() >= 100))
                //.doOnNext(map -> Assert.assertTrue(map.size() >= 262)) // MySQL 8.0
                .block();

        Mono.from(metaData.sqlKeyWords(false))
                .doOnNext(map -> Assert.assertTrue(map.size() >= 100))
                // .doOnNext(map -> Assert.assertTrue(map.size() >= 752)) // MySQL 8.0
                // .doOnNext(this::printSqlKeyWordMap)
                .block();
    }


    /**
     * @see #tablesOfSchema(DatabaseMetaData, Function)
     */
    @DataProvider(name = "tablesOfSchemaProvider", parallel = true)
    public final Object[][] tablesOfSchemaProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String key;
        key = keyNameOfSession(targetMethod);

        final int currentInvocationCount = targetMethod.getCurrentInvocationCount() + 1;

        final boolean closeSession;
        closeSession = currentInvocationCount == targetMethod.getInvocationCount();

        Object sessionHolder;
        sessionHolder = context.getAttribute(key);
        final DatabaseSession session;
        if (sessionHolder instanceof TestSessionHolder) {
            session = ((TestSessionHolder) sessionHolder).session;
        } else {
            session = Mono.from(sessionFactory.localSession())
                    .block();
            assert session != null;
        }

        context.setAttribute(key, new TestSessionHolder(session, closeSession));


        final Function<Option<?>, ?> optionFunc;
        switch ((currentInvocationCount % 10)) {
            case 1:
                optionFunc = Option.EMPTY_OPTION_FUNC;
                break;
            case 2:
                optionFunc = option -> {
                    if (option == Option.NAME) {
                        return "mysql_types";
                    }
                    return null;
                };
                break;
            case 3:
                optionFunc = option -> {
                    if (option == Option.NAME) {
                        return "%";
                    }
                    return null;
                };
                break;
            case 4:
                optionFunc = option -> {
                    if (option == Option.NAME) {
                        return "mysql_types,bank_user";
                    }
                    return null;
                };
                break;
            case 5:
                optionFunc = option -> {
                    if (option == Option.TYPE_NAME) {
                        return TableMeta.TABLE;
                    }
                    return null;
                };
                break;
            case 6:
                optionFunc = option -> {
                    if (option == Option.TYPE_NAME) {
                        return "%";
                    }
                    return null;
                };
                break;
            case 7:
                optionFunc = option -> {
                    if (option == Option.TYPE_NAME) {
                        return "TABLE,VIEW,SYSTEM TABLE";
                    }
                    return null;
                };
                break;
            case 8:
                optionFunc = option -> {
                    final Object value;
                    if (option == Option.NAME) {
                        value = "mysql_types";
                    } else if (option == Option.TYPE_NAME) {
                        value = "%";
                    } else {
                        value = null;
                    }
                    return value;
                };
                break;
            case 9:
                optionFunc = option -> {
                    final Object value;
                    if (option == Option.NAME) {
                        value = "%";
                    } else if (option == Option.TYPE_NAME) {
                        value = "TA_LE";
                    } else {
                        value = null;
                    }
                    return value;
                };
                break;
            default:
                optionFunc = option -> {
                    final Object value;
                    if (option == Option.NAME) {
                        value = "mysql_types,bank_user";
                    } else if (option == Option.TYPE_NAME) {
                        value = "TABLE,VIEW,SYSTEM TABLE";
                    } else {
                        value = null;
                    }
                    return value;
                };
        }

        return new Object[][]{{session.databaseMetaData(), optionFunc}};

    }


    /**
     * @see #columnsOfTable(DatabaseMetaData, Function)
     */
    @DataProvider(name = "columnsOfTableProvider", parallel = true)
    public final Object[][] columnsOfTableProvider(final ITestNGMethod targetMethod, final ITestContext context) {
        final String key;
        key = keyNameOfSession(targetMethod);

        final int currentInvocationCount = targetMethod.getCurrentInvocationCount() + 1;

        final boolean closeSession;
        closeSession = currentInvocationCount == targetMethod.getInvocationCount();

        Object sessionHolder;
        sessionHolder = context.getAttribute(key);
        final DatabaseSession session;
        if (sessionHolder instanceof TestSessionHolder) {
            session = ((TestSessionHolder) sessionHolder).session;
        } else {
            session = Mono.from(sessionFactory.localSession())
                    .block();
            assert session != null;
        }

        context.setAttribute(key, new TestSessionHolder(session, closeSession));


        final Function<Option<?>, ?> optionFunc;
        switch ((currentInvocationCount % 4)) {
            case 1:
                optionFunc = Option.EMPTY_OPTION_FUNC;
                break;
            case 2:
                optionFunc = option -> {
                    if (option == Option.NAME) {
                        return "my_special_enum";
                    }
                    return null;
                };
                break;
            case 3:
                optionFunc = option -> {
                    if (option == Option.NAME) {
                        return "my%";
                    }
                    return null;
                };
                break;
            default:
                optionFunc = option -> {
                    if (option == Option.NAME) {
                        return "my_special_enum,my_enum,my_set,my_datetime,my_date";
                    }
                    return null;
                };
        }

        return new Object[][]{{session.databaseMetaData(), optionFunc}};

    }


    /**
     * @see #sqlKeyWords(DatabaseMetaData)
     */
    private void printSqlKeyWordMap(final Map<String, Boolean> map) {
        final StringBuilder builder = new StringBuilder();
        boolean output = false;
        for (Map.Entry<String, Boolean> e : map.entrySet()) {
            if (output) {
                builder.append(System.lineSeparator());
            }
            builder.append(e.getKey())
                    .append(" : reserved is ")
                    .append(e.getValue());

            if (!output) {
                output = true;
            }
        }

        LOG.info("{}", builder);


    }


}
