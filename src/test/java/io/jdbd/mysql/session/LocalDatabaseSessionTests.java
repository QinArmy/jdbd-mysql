package io.jdbd.mysql.session;


import io.jdbd.session.*;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * <p>
 * This class is the test class of {@link MySQLLocalDatabaseSession}
 * <br/>
 * <p>
 * All test method's session parameter is created by {@link #createLocalSession(ITestNGMethod, ITestContext)},
 * and is closed by {@link #closeSessionAfterTest(Method, ITestContext)}
 * <br/>
 */
@Test(dataProvider = "localSessionProvider")
public class LocalDatabaseSessionTests extends SessionTestSupport {


    /**
     * @see LocalDatabaseSession#startTransaction(TransactionOption, HandleMode)
     * @see LocalDatabaseSession#commit(Function)
     */
    @Test(invocationCount = 2)
    public void startTransactionAndCommit(final LocalDatabaseSession session, final boolean readOnly) {
        final TransactionOption txOption;
        txOption = TransactionOption.builder()
                .option(Option.READ_ONLY, readOnly)
                .option(Option.ISOLATION, Isolation.REPEATABLE_READ)
                .option(Option.WITH_CONSISTENT_SNAPSHOT, Boolean.TRUE)
                .build();

        final Map<Option<?>, ?> optionMap = Collections.singletonMap(Option.CHAIN, Boolean.TRUE);


        Mono.from(session.startTransaction(txOption))

                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT), Boolean.TRUE);
                })

                .flatMap(s -> Mono.from(session.commit()))
                .doOnSuccess(s -> Assert.assertFalse(s.inTransaction()))

                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertFalse(s.inTransaction());
                    Assert.assertNotNull(s.isolation());
                    Assert.assertFalse(s.isReadOnly());
                    Assert.assertNull(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT));
                })

                .then(Mono.from(session.startTransaction(txOption)))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT), Boolean.TRUE);
                })

                .flatMap(s -> Mono.from(session.commit(optionMap::get)))  // COMMIT AND CHAIN
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());  // due to COMMIT AND CHAIN, so session still in transaction block.
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT), Boolean.TRUE);
                })

                .flatMap(s -> Mono.from(session.commit()))
                .doOnSuccess(s -> Assert.assertFalse(s.inTransaction()))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertFalse(s.inTransaction());
                    Assert.assertNotNull(s.isolation());
                    Assert.assertFalse(s.isReadOnly());
                    Assert.assertNull(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT));
                })
                .block();
    }

    /**
     * @see LocalDatabaseSession#startTransaction(TransactionOption, HandleMode)
     * @see LocalDatabaseSession#rollback(Function)
     */
    @Test(invocationCount = 2)
    public void startTransactionAndRollback(final LocalDatabaseSession session, final boolean readOnly) {
        final TransactionOption txOption;
        txOption = TransactionOption.builder()
                .option(Option.READ_ONLY, readOnly)
                .option(Option.ISOLATION, Isolation.REPEATABLE_READ)
                .option(Option.WITH_CONSISTENT_SNAPSHOT, Boolean.TRUE)
                .build();

        final Map<Option<?>, ?> optionMap = Collections.singletonMap(Option.CHAIN, Boolean.TRUE);


        Mono.from(session.startTransaction(txOption))

                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT), Boolean.TRUE);
                })

                .flatMap(s -> Mono.from(session.rollback()))
                .doOnSuccess(s -> Assert.assertFalse(s.inTransaction()))

                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertFalse(s.inTransaction());
                    Assert.assertNotNull(s.isolation());
                    Assert.assertFalse(s.isReadOnly());
                    Assert.assertNull(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT));
                })

                .then(Mono.from(session.startTransaction(txOption)))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT), Boolean.TRUE);
                })

                .flatMap(s -> Mono.from(session.rollback(optionMap::get)))  // ROLLBACK AND CHAIN
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());  // due to ROLLBACK AND CHAIN, so session still in transaction block.
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT), Boolean.TRUE);
                })

                .flatMap(s -> Mono.from(session.rollback()))
                .doOnSuccess(s -> Assert.assertFalse(s.inTransaction()))

                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertFalse(s.inTransaction());
                    Assert.assertNotNull(s.isolation());
                    Assert.assertFalse(s.isReadOnly());
                    Assert.assertNull(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT));
                })
                .block();
    }

    /**
     * @see LocalDatabaseSession#startTransaction(TransactionOption, HandleMode)
     * @see LocalDatabaseSession#commit(Function)
     */
    @Test(invocationCount = 2)
    public void startTransactionAndCommitRelease(final LocalDatabaseSession session, final boolean readOnly) {
        final TransactionOption txOption;
        txOption = TransactionOption.builder()
                .option(Option.READ_ONLY, readOnly)
                .option(Option.ISOLATION, Isolation.REPEATABLE_READ)
                .option(Option.WITH_CONSISTENT_SNAPSHOT, Boolean.TRUE)
                .build();

        final Map<Option<?>, ?> optionMap = Collections.singletonMap(Option.RELEASE, Boolean.TRUE);

        Mono.from(session.startTransaction(txOption))

                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT), Boolean.TRUE);
                })

                .flatMap(s -> Mono.from(session.commit(optionMap::get))) // COMMIT RELEASE
                .delayElement(Duration.ofMillis(200)) // wait for close
                .block();

        Assert.assertTrue(session.isClosed());

    }

    /**
     * @see LocalDatabaseSession#startTransaction(TransactionOption, HandleMode)
     * @see LocalDatabaseSession#rollback(Function)
     */
    @Test(invocationCount = 2)
    public void startTransactionAndRollbackRelease(final LocalDatabaseSession session, final boolean readOnly) {
        final TransactionOption txOption;
        txOption = TransactionOption.builder()
                .option(Option.READ_ONLY, readOnly)
                .option(Option.ISOLATION, Isolation.REPEATABLE_READ)
                .option(Option.WITH_CONSISTENT_SNAPSHOT, Boolean.TRUE)
                .build();

        final Map<Option<?>, ?> optionMap = Collections.singletonMap(Option.RELEASE, Boolean.TRUE);

        Mono.from(session.startTransaction(txOption))

                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.valueOf(Option.WITH_CONSISTENT_SNAPSHOT), Boolean.TRUE);
                })

                .flatMap(s -> Mono.from(session.rollback(optionMap::get)))  // ROLLBACK RELEASE
                .delayElement(Duration.ofMillis(200)) // wait for close
                .block();

        Assert.assertTrue(session.isClosed());

    }


}
