package io.jdbd.vendor.task;

import io.jdbd.JdbdException;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslHandler;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.netty.Connection;
import reactor.netty.NettyPipeline;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.function.Consumer;

public abstract class CommunicationTaskExecutor<T extends TaskAdjutant> implements CoreSubscriber<ByteBuf>
        , TaskExecutor<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CommunicationTaskExecutor.class);

    private final Queue<CommunicationTask> taskQueue = Queues.<CommunicationTask>small().get();

    protected final Connection connection;

    protected final EventLoop eventLoop;

    protected final ByteBufAllocator allocator;

    protected final T taskAdjutant;

    //private final MySQLTaskAdjutantWrapper taskAdjutant;

    // non-volatile ,all modify in netty EventLoop
    protected ByteBuf cumulateBuffer;

    private final TaskSignal taskSignal;

    private Subscription upstream;

    private CommunicationTask currentTask;

    private Throwable taskErrorMethodError;


    protected CommunicationTaskExecutor(Connection connection) {
        this.connection = connection;
        this.eventLoop = connection.channel().eventLoop();
        this.allocator = connection.channel().alloc();
        this.taskSignal = new TaskSingleImpl(this);

        this.taskAdjutant = createTaskAdjutant();

        connection.inbound()
                .receive()
                .retain() // for cumulate
                .subscribe(this);

    }

    @Override
    public final T getAdjutant() {
        return this.taskAdjutant;
    }

    @Override
    public final void onSubscribe(Subscription s) {
        this.upstream = s;
        s.request(Long.MAX_VALUE);
    }


    @Override
    public final void onNext(final ByteBuf byteBufFromPeer) {
        if (this.eventLoop.inEventLoop()) {
            doOnNextInEventLoop(byteBufFromPeer);
        } else {
            this.eventLoop.execute(() -> doOnNextInEventLoop(byteBufFromPeer));
        }
    }

    @Override
    public final void onError(Throwable t) {
        if (this.eventLoop.inEventLoop()) {
            doOnErrorInEventLoop(t);
        } else {
            this.eventLoop.execute(() -> doOnErrorInEventLoop(t));
        }
    }

    @Override
    public final void onComplete() {
        if (this.eventLoop.inEventLoop()) {
            doOnCompleteInEventLoop();
        } else {
            this.eventLoop.execute(this::doOnCompleteInEventLoop);
        }
    }

    /*################################## blow protected method ##################################*/

    /**
     * must invoke in {@link #eventLoop}
     */
    protected final void drainToTask() {
        final ByteBuf cumulateBuffer = this.cumulateBuffer;
        if (cumulateBuffer == null) {
            return;
        }
        CommunicationTask currentTask;
        while ((currentTask = this.currentTask) != null) {
            if (!cumulateBuffer.isReadable()) {
                break;
            }
            if (currentTask.decode(cumulateBuffer, this::updateServerStatus)) {
                this.currentTask = null; // current task end.
                if (!startHeadIfNeed()) {  // start next task
                    break;
                }
            } else {
                Publisher<ByteBuf> bufPublisher = currentTask.moreSendPacket();
                if (bufPublisher != null) {
                    // send packet
                    sendPacket(currentTask, bufPublisher);
                }
                break;
            }
        }
    }


    protected abstract void updateServerStatus(Object serverStatus);

    protected abstract T createTaskAdjutant();


    /*################################## blow private method ##################################*/

    final void syncPushTask(CommunicationTask task, Consumer<Boolean> offerCall)
            throws IllegalStateException {
        if (!this.eventLoop.inEventLoop()) {
            throw new IllegalStateException("Current thread not in EventLoop.");
        }
        if (!this.connection.channel().isActive()) {
            throw new IllegalStateException("Cannot summit CommunicationTask because TCP connection closed.");
        }
        if (this.taskQueue.offer(task)) {
            offerCall.accept(Boolean.TRUE);
            if (startHeadIfNeed()) {
                drainToTask();
            }
        } else {
            offerCall.accept(Boolean.FALSE);
        }
    }


    /**
     * @see #onNext(ByteBuf)
     */
    private void doOnNextInEventLoop(final ByteBuf byteBufFromPeer) {
        if (byteBufFromPeer == this.cumulateBuffer) {
            // subclass bug
            throw new IllegalStateException("previous cumulateBuffer handle error.");
        }
        //  cumulate Buffer
        ByteBuf cumulateBuffer = this.cumulateBuffer;
        if (cumulateBuffer == null) {
            cumulateBuffer = byteBufFromPeer;
        } else {
            cumulateBuffer = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(
                    this.allocator, cumulateBuffer, byteBufFromPeer);
        }
        this.cumulateBuffer = cumulateBuffer;
        drainToTask();
    }

    private void doOnErrorInEventLoop(Throwable e) {
        LOG.debug("channel upstream error.");

        if (!this.connection.channel().isActive()) {
            CommunicationTask task = this.currentTask;
            final JdbdException exception = JdbdExceptions.wrap(e
                    , "TCP connection close,cannot execute CommunicationTask.");
            if (task != null) {
                this.currentTask = null;
                task.error(exception);
            }
            while ((task = this.taskQueue.poll()) != null) {
                task.error(exception);
            }
        } else {
            // TODO optimize handle netty Handler error.
            CommunicationTask task = this.currentTask;
            if (task != null) {
                this.currentTask = null;
                task.error(JdbdExceptions.wrap(e, "Channel upstream throw error."));
            }

        }
    }

    /**
     * must invoke in {@link #eventLoop}
     */
    private void doOnCompleteInEventLoop() {
        LOG.debug("Channel close.");
        CommunicationTask task = this.currentTask;
        if (task != null) {
            task.onChannelClose();
        }
        while ((task = this.taskQueue.poll()) != null) {
            task.onChannelClose();
        }
    }

    /**
     * must invoke in {@link #eventLoop}
     *
     * @return true : need decode
     */
    private boolean startHeadIfNeed() {
        CommunicationTask currentTask = this.currentTask;
        if (currentTask != null) {
            return false;
        }

        Throwable taskErrorMethodError = this.taskErrorMethodError;

        boolean needDecode = false;
        while ((currentTask = this.taskQueue.poll()) != null) {

            if (currentTask.getTaskPhase() != CommunicationTask.TaskPhase.SUBMITTED) {
                currentTask.error(new TaskStatusException("CommunicationTask[%s] phase isn't SUBMITTED,cannot start."
                        , currentTask));
                continue;
            }
            if (taskErrorMethodError != null) {
                currentTask.error(JdbdExceptions.wrap(taskErrorMethodError
                        , "last task error(Throwable) method throw error."));
                // TODO optimize handle last task error.
                this.taskErrorMethodError = null;
                taskErrorMethodError = null;
                continue;
            }

            this.currentTask = currentTask;
            if (currentTask instanceof ConnectionTask) {
                ((ConnectionTask) currentTask).sslHandlerConsumer(this::addSslHandler);
            }
            Publisher<ByteBuf> bufPublisher;
            try {
                bufPublisher = currentTask.start(this.taskSignal);
            } catch (Throwable e) {
                handleTaskError(currentTask, new TaskStatusException(e, "%s start failure.", currentTask));
                continue;
            }
            if (bufPublisher != null) {
                // send packet
                sendPacket(currentTask, bufPublisher);
            } else {
                this.upstream.request(128L);
                needDecode = true;
            }
            break;
        }
        return needDecode;
    }

    private void addSslHandler(SslHandler sslHandler) {

        ChannelPipeline pipeline = this.connection.channel().pipeline();
        if (pipeline.get(NettyPipeline.SslHandler) != null) {
            LOG.warn("duplicate add {}", SslHandler.class.getName());
            return;
        }
        LOG.debug("add ssl handler");
        if (pipeline.get(NettyPipeline.ProxyHandler) != null) {
            pipeline.addAfter(NettyPipeline.ProxyHandler, NettyPipeline.SslHandler, sslHandler);
        } else {
            pipeline.addFirst(NettyPipeline.SslHandler, sslHandler);
        }


    }

    private void doSendPacketSignal(final MonoSink<Void> sink, final CommunicationTask signalTask) {
        LOG.debug("{} send packet signal", signalTask);
        if (signalTask == this.currentTask) {
            Publisher<ByteBuf> packetPublisher;
            try {
                packetPublisher = signalTask.moreSendPacket();
            } catch (Throwable e) {
                sink.error(new TaskStatusException(e, "%s moreSendPacket() method throw error.", signalTask));
                return;
            }
            if (packetPublisher != null) {
                sendPacket(signalTask, packetPublisher);
            }
            sink.success();
        } else {
            sink.error(new IllegalArgumentException(String.format("task[%s] isn't current task.", signalTask)));
        }
    }

    private void sendPacket(final CommunicationTask headTask, final Publisher<ByteBuf> packetPublisher) {
        Mono.from(this.connection.outbound().send(packetPublisher))
                .doOnError(e -> {
                    if (this.eventLoop.inEventLoop()) {
                        handleTaskError(headTask, e);
                    } else {
                        this.eventLoop.execute(() -> handleTaskError(headTask, e));
                    }
                })
                .doOnSuccess(v -> {
                    if (this.eventLoop.inEventLoop()) {
                        handleTaskPacketSendSuccess(headTask);
                    } else {
                        this.eventLoop.execute(() -> handleTaskPacketSendSuccess(headTask));
                    }
                })
                .subscribe(v -> this.upstream.request(128L))
        ;
    }

    /**
     * @see #sendPacket(CommunicationTask, Publisher)
     * @see #handleTaskPacketSendSuccess(CommunicationTask)
     * @see #startHeadIfNeed()
     * @see #doSendPacketSignal(MonoSink, CommunicationTask)
     */
    private void handleTaskError(final CommunicationTask task, final Throwable e) {
        try {
            task.error(JdbdExceptions.wrap(e));
            currentTaskEndIfNeed(task);
        } catch (Throwable te) {
            this.taskErrorMethodError = new TaskStatusException(e, "%s error(Throwable) method throw error.", task);
        }

    }

    private void handleTaskPacketSendSuccess(final CommunicationTask task) {
        try {
            if (task.onSendSuccess()) {
                currentTaskEndIfNeed(task);
            }
        } catch (Throwable e) {
            handleTaskError(task, new TaskStatusException(e, "%s onSendSuccess() method throw error.", task));
        }
    }

    private void currentTaskEndIfNeed(final CommunicationTask task) {
        if (task == this.currentTask) {

            ByteBuf cumulate = this.cumulateBuffer;
            if (cumulate != null) {
                if (cumulate.isReadable()) {
                    cumulate.readerIndex(cumulate.writerIndex());
                }
                cumulate.release();
                this.cumulateBuffer = null;
            }

            this.currentTask = null;

            if (startHeadIfNeed()) {
                drainToTask();
            }

        }


    }

    /*################################## blow private static class ##################################*/

    protected static abstract class AbstractTaskAdjutant implements TaskAdjutant {

        private final CommunicationTaskExecutor<?> taskExecutor;

        protected AbstractTaskAdjutant(CommunicationTaskExecutor<?> taskExecutor) {
            this.taskExecutor = taskExecutor;
        }

        @Override
        public final boolean inEventLoop() {
            return this.taskExecutor.eventLoop.inEventLoop();
        }

        @Override
        public final void syncSubmitTask(CommunicationTask task, Consumer<Boolean> offerCall)
                throws IllegalStateException {
            this.taskExecutor.syncPushTask(task, offerCall);
        }

        @Override
        public final void execute(Runnable runnable) {
            this.taskExecutor.eventLoop.execute(runnable);
        }

        @Override
        public final ByteBufAllocator allocator() {
            return this.taskExecutor.allocator;
        }

    }

    private static final class TaskSingleImpl implements TaskSignal {

        private final CommunicationTaskExecutor<?> taskExecutor;

        private TaskSingleImpl(CommunicationTaskExecutor<?> taskExecutor) {
            this.taskExecutor = taskExecutor;
        }


        @Override
        public Mono<Void> sendPacket(final CommunicationTask task) {
            return Mono.create(sink -> {
                if (this.taskExecutor.eventLoop.inEventLoop()) {
                    this.taskExecutor.doSendPacketSignal(sink, task);
                } else {
                    this.taskExecutor.eventLoop.execute(() -> this.taskExecutor.doSendPacketSignal(sink, task));
                }
            });
        }
    }


}
