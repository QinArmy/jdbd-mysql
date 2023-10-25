package io.jdbd.mysql.session;


import io.jdbd.session.*;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * <p>
 * This class is the test class of {@link MySQLRmDatabaseSession}
 * <br/>
 * <p>
 * All test method's session parameter is created by {@link #createRmSession(ITestNGMethod, ITestContext)},
 * and is closed by {@link #closeSessionAfterTest(Method, ITestContext)}
 * <br/>
 */
@Test(dataProvider = "rmSessionProvider")
public class RmDatabaseSessionTests extends SessionTestSupport {


    private static final AtomicInteger XID_SEQUENCED_ID = new AtomicInteger(0);


    /**
     * @see io.jdbd.session.RmDatabaseSession#start(Xid, int, TransactionOption)
     * @see io.jdbd.session.RmDatabaseSession#end(Xid, int, Function)
     * @see io.jdbd.session.RmDatabaseSession#commit(Xid, int, Function)
     * @see io.jdbd.session.RmDatabaseSession#rollback(Xid, Function)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Test(invocationCount = 2)
    public void onePhaseXaTransaction(final RmDatabaseSession session, final boolean readOnly) {
        final TransactionOption txOption;
        txOption = TransactionOption.option(Isolation.REPEATABLE_READ, readOnly);

        final String gtrid, bqual;
        gtrid = "QinArmy" + XID_SEQUENCED_ID.getAndIncrement()
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);

        bqual = "army" + XID_SEQUENCED_ID.getAndIncrement()
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);

        final Xid xid = Xid.from(gtrid, bqual, 1);

        Mono.from(session.start(xid, RmDatabaseSession.TM_NO_FLAGS, txOption))

                .flatMap(s -> Mono.from(session.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.valueOf(Option.XA_STATES), XaStates.ACTIVE);

                    Assert.assertEquals(s.valueOf(Option.XID), xid);
                    Assert.assertEquals(s.valueOf(Option.XA_FLAGS), RmDatabaseSession.TM_NO_FLAGS);
                })

                .then(Mono.defer(() -> Mono.from(session.end(xid, RmDatabaseSession.TM_SUCCESS))))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.valueOf(Option.XA_STATES), XaStates.IDLE);

                    Assert.assertEquals(s.valueOf(Option.XID), xid);
                    Assert.assertEquals(s.valueOf(Option.XA_FLAGS), RmDatabaseSession.TM_SUCCESS);
                })

                .then(Mono.defer(() -> Mono.from(session.commit(xid, RmDatabaseSession.TM_ONE_PHASE))))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertFalse(s.inTransaction());
                    Assert.assertFalse(s.isReadOnly());
                    Assert.assertNotNull(s.isolation());
                    Assert.assertNull(s.valueOf(Option.XA_STATES));

                    Assert.assertNull(s.valueOf(Option.XID));
                    Assert.assertNull(s.valueOf(Option.XA_FLAGS));
                })


                .block();


    }

    /**
     * @see io.jdbd.session.RmDatabaseSession#start(Xid, int, TransactionOption)
     * @see io.jdbd.session.RmDatabaseSession#end(Xid, int, Function)
     * @see io.jdbd.session.RmDatabaseSession#prepareStatement(String)
     * @see io.jdbd.session.RmDatabaseSession#commit(Xid, int, Function)
     * @see io.jdbd.session.RmDatabaseSession#rollback(Xid, Function)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Test(invocationCount = 2)
    public void simpleXaTransaction(final RmDatabaseSession session, final boolean readOnly) {
        final TransactionOption txOption;
        txOption = TransactionOption.option(Isolation.REPEATABLE_READ, readOnly);

        final String gtrid, bqual;
        gtrid = "QinArmy" + XID_SEQUENCED_ID.getAndIncrement()
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);

        bqual = "army" + XID_SEQUENCED_ID.getAndIncrement()
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);

        final Xid xid = Xid.from(gtrid, bqual, 1);

        Mono.from(session.start(xid, RmDatabaseSession.TM_NO_FLAGS, txOption))

                .flatMap(s -> Mono.from(session.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.valueOf(Option.XA_STATES), XaStates.ACTIVE);

                    Assert.assertEquals(s.valueOf(Option.XID), xid);
                    Assert.assertEquals(s.valueOf(Option.XA_FLAGS), RmDatabaseSession.TM_NO_FLAGS);
                })

                .then(Mono.defer(() -> Mono.from(session.end(xid, RmDatabaseSession.TM_SUCCESS))))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.valueOf(Option.XA_STATES), XaStates.IDLE);

                    Assert.assertEquals(s.valueOf(Option.XID), xid);
                    Assert.assertEquals(s.valueOf(Option.XA_FLAGS), RmDatabaseSession.TM_SUCCESS);
                })

                .then(Mono.defer(() -> Mono.from(session.prepare(xid))))
                .then(Mono.defer(() -> Mono.from(session.transactionInfo())))
                .doOnSuccess(s -> {
                    Assert.assertFalse(s.inTransaction());
                    Assert.assertFalse(s.isReadOnly());
                    Assert.assertNotNull(s.isolation());
                    Assert.assertNull(s.valueOf(Option.XA_STATES));

                    Assert.assertNull(s.valueOf(Option.XID));
                    Assert.assertNull(s.valueOf(Option.XA_FLAGS));
                })

                .then(Mono.defer(() -> Mono.from(session.commit(xid, RmDatabaseSession.TM_NO_FLAGS))))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertFalse(s.inTransaction());
                    Assert.assertFalse(s.isReadOnly());
                    Assert.assertNotNull(s.isolation());
                    Assert.assertNull(s.valueOf(Option.XA_STATES));

                    Assert.assertNull(s.valueOf(Option.XID));
                    Assert.assertNull(s.valueOf(Option.XA_FLAGS));
                })

                // following rollback

                .then(Mono.from(session.start(xid, RmDatabaseSession.TM_NO_FLAGS, txOption)))

                .flatMap(s -> Mono.from(session.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.valueOf(Option.XA_STATES), XaStates.ACTIVE);

                    Assert.assertEquals(s.valueOf(Option.XID), xid);
                    Assert.assertEquals(s.valueOf(Option.XA_FLAGS), RmDatabaseSession.TM_NO_FLAGS);
                })

                .then(Mono.defer(() -> Mono.from(session.end(xid, RmDatabaseSession.TM_SUCCESS))))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.valueOf(Option.XA_STATES), XaStates.IDLE);

                    Assert.assertEquals(s.valueOf(Option.XID), xid);
                    Assert.assertEquals(s.valueOf(Option.XA_FLAGS), RmDatabaseSession.TM_SUCCESS);
                })

                .then(Mono.defer(() -> Mono.from(session.prepare(xid))))
                .then(Mono.defer(() -> Mono.from(session.transactionInfo())))
                .doOnSuccess(s -> {
                    Assert.assertFalse(s.inTransaction());
                    Assert.assertFalse(s.isReadOnly());
                    Assert.assertNotNull(s.isolation());
                    Assert.assertNull(s.valueOf(Option.XA_STATES));

                    Assert.assertNull(s.valueOf(Option.XID));
                    Assert.assertNull(s.valueOf(Option.XA_FLAGS));
                })

                .then(Mono.defer(() -> Mono.from(session.rollback(xid))))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertFalse(s.inTransaction());
                    Assert.assertFalse(s.isReadOnly());
                    Assert.assertNotNull(s.isolation());
                    Assert.assertNull(s.valueOf(Option.XA_STATES));

                    Assert.assertNull(s.valueOf(Option.XID));
                    Assert.assertNull(s.valueOf(Option.XA_FLAGS));
                })

                .block();


    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Test(invocationCount = 4, expectedExceptions = XaException.class)
    public void rollbackOnlyOnePhase(final RmDatabaseSession session, final boolean readOnly) {

        final TransactionOption txOption;
        txOption = TransactionOption.option(Isolation.REPEATABLE_READ, readOnly);

        final String gtrid, bqual;
        gtrid = "QinArmy" + XID_SEQUENCED_ID.getAndIncrement()
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);

        bqual = "army" + XID_SEQUENCED_ID.getAndIncrement()
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);

        final Xid xid = Xid.from(gtrid, bqual, 1);

        Mono.from(session.start(xid, RmDatabaseSession.TM_NO_FLAGS, txOption))

                .flatMap(s -> Mono.from(session.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.valueOf(Option.XA_STATES), XaStates.ACTIVE);

                    Assert.assertEquals(s.valueOf(Option.XID), xid);
                    Assert.assertEquals(s.valueOf(Option.XA_FLAGS), RmDatabaseSession.TM_NO_FLAGS);
                })

                .then(Mono.defer(() -> Mono.from(session.end(xid, RmDatabaseSession.TM_FAIL))))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.valueOf(Option.XA_STATES), XaStates.IDLE);

                    Assert.assertEquals(s.valueOf(Option.XID), xid);
                    Assert.assertEquals(s.valueOf(Option.XA_FLAGS), RmDatabaseSession.TM_FAIL);
                })

                .then(Mono.defer(() -> Mono.from(session.commit(xid, RmDatabaseSession.TM_ONE_PHASE))))
                .block();


    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Test(invocationCount = 4, expectedExceptions = XaException.class)
    public void rollbackOnly(final RmDatabaseSession session, final boolean readOnly) {

        final TransactionOption txOption;
        txOption = TransactionOption.option(Isolation.REPEATABLE_READ, readOnly);

        final String gtrid, bqual;
        gtrid = "QinArmy" + XID_SEQUENCED_ID.getAndIncrement()
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);

        bqual = "army" + XID_SEQUENCED_ID.getAndIncrement()
                + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);

        final Xid xid = Xid.from(gtrid, bqual, 1);

        Mono.from(session.start(xid, RmDatabaseSession.TM_NO_FLAGS, txOption))

                .flatMap(s -> Mono.from(session.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.valueOf(Option.XA_STATES), XaStates.ACTIVE);

                    Assert.assertEquals(s.valueOf(Option.XID), xid);
                    Assert.assertEquals(s.valueOf(Option.XA_FLAGS), RmDatabaseSession.TM_NO_FLAGS);
                })

                .then(Mono.defer(() -> Mono.from(session.end(xid, RmDatabaseSession.TM_FAIL))))
                .flatMap(s -> Mono.from(s.transactionInfo()))
                .doOnSuccess(s -> {
                    Assert.assertTrue(s.inTransaction());
                    Assert.assertEquals(s.isReadOnly(), readOnly);
                    Assert.assertEquals(s.isolation(), Isolation.REPEATABLE_READ);
                    Assert.assertEquals(s.valueOf(Option.XA_STATES), XaStates.IDLE);

                    Assert.assertEquals(s.valueOf(Option.XID), xid);
                    Assert.assertEquals(s.valueOf(Option.XA_FLAGS), RmDatabaseSession.TM_FAIL);
                })

                .then(Mono.defer(() -> Mono.from(session.prepare(xid))))
                .then(Mono.defer(() -> Mono.from(session.transactionInfo())))
                .doOnSuccess(s -> {
                    Assert.assertFalse(s.inTransaction());
                    Assert.assertFalse(s.isReadOnly());
                    Assert.assertNotNull(s.isolation());
                    Assert.assertNull(s.valueOf(Option.XA_STATES));

                    Assert.assertNull(s.valueOf(Option.XID));
                    Assert.assertNull(s.valueOf(Option.XA_FLAGS));
                })

                .then(Mono.defer(() -> Mono.from(session.commit(xid, RmDatabaseSession.TM_NO_FLAGS))))
                .block();


    }

    /**
     * @see RmDatabaseSession#recover(int, Function)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html">XA Transaction SQL Statements</a>
     */
    @Test(dependsOnMethods = {"rollbackOnlyOnePhase", "rollbackOnly"})
    public void recover(final RmDatabaseSession session) {

        Flux.from(session.recover())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .doOnNext(xid -> LOG.info("rollback {} ...", xid))
                .flatMap(session::rollback)
                .collectList()
                .block();

    }


}
