package io.jdbd.vendor;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.TaskQueueOverflowException;
import io.jdbd.lang.Nullable;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;

import java.util.function.Consumer;

public abstract class AbstractCommunicationTask implements CommunicationTask<ByteBuf> {

    final TaskAdjutant executorAdjutant;

    private TaskPhase taskPhase;

    protected AbstractCommunicationTask(TaskAdjutant executorAdjutant) {
        this.executorAdjutant = executorAdjutant;
    }

    @Nullable
    @Override
    public final Publisher<ByteBuf> start(TaskSignal<ByteBuf> signal) {
        if (!this.executorAdjutant.inEventLoop()) {
            throw new IllegalStateException("start() isn't in EventLoop.");
        }
        if (this.taskPhase != TaskPhase.SUBMITTED) {
            throw new IllegalStateException("taskPhase not null");
        }
        Publisher<ByteBuf> publisher = internalStart(signal);
        this.taskPhase = TaskPhase.STARTED;
        return publisher;
    }

    @Override
    public final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (!this.executorAdjutant.inEventLoop()) {
            throw new IllegalStateException("decode(ByteBuf) isn't in EventLoop.");
        }
        if (this.taskPhase != TaskPhase.STARTED) {
            throw new IllegalStateException("Communication task not start.");
        }
        boolean taskEnd;
        taskEnd = internalDecode(cumulateBuffer, serverStatusConsumer);
        if (taskEnd) {
            this.taskPhase = TaskPhase.END;
        }
        return taskEnd;
    }

    @Nullable
    @Override
    public final Publisher<ByteBuf> error(Throwable e) {
        this.taskPhase = TaskPhase.END;
        return internalError(e);
    }

    @Override
    public final TaskPhase getTaskPhase() {
        return this.taskPhase;
    }

    @Override
    public final void onChannelClose() {
        this.taskPhase = TaskPhase.END;
        // TODO optimize
        internalOnChannelClose();

    }

    protected final void submit(Consumer<Throwable> consumer) {
        if (this.executorAdjutant.inEventLoop()) {
            syncSubmitTask(consumer);
        } else {
            this.executorAdjutant.execute(() -> syncSubmitTask(consumer));
        }
    }

    @Nullable
    protected Publisher<ByteBuf> internalError(Throwable e) {
        return null;
    }

    protected void internalOnChannelClose() {

    }

    @Nullable
    protected abstract Publisher<ByteBuf> internalStart(TaskSignal<ByteBuf> signal);

    protected abstract boolean internalDecode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);


    /*################################## blow package static method ##################################*/

    /*################################## blow private method ##################################*/


    private void syncSubmitTask(Consumer<Throwable> consumer) {
        if (this.taskPhase != null) {
            throw new IllegalStateException("Communication task have submitted.");
        }
        try {
            this.executorAdjutant.syncSubmitTask(this, success -> {
                if (success) {
                    this.taskPhase = TaskPhase.SUBMITTED;
                } else {
                    consumer.accept(new TaskQueueOverflowException("Communication task queue overflow,cant' execute task."));
                }
            });
        } catch (JdbdNonSQLException e) {
            consumer.accept(e);
        }

    }


}
