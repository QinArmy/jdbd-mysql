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

package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.UnrecognizedCollationException;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStates;
import io.jdbd.result.ResultItem;
import io.jdbd.statement.TimeoutException;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.task.TimeoutTask;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

/**
 * <p>
 * This class is base class of below:
 *     <ul>
 *         <li>{@link ComQueryTask}</li>
 *         <li>{@link ComPreparedTask}</li>
 *     </ul>
 * <br/>
 *
 * @see ComQueryTask
 * @see ComPreparedTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
abstract class MySQLCommandTask extends MySQLTask implements StmtTask {

    final Logger log = LoggerFactory.getLogger(getClass());

    final ResultSink sink;

    final int capability;

    private TimeoutTask timeoutTask;

    private final ResultSetReader resultSetReader;

    private int sequenceId = 0;

    private int resultNo = 0;

    private boolean downstreamCanceled;

    private List<Path> bigColumnPathList;

    MySQLCommandTask(TaskAdjutant adjutant, final ResultSink sink) {
        super(adjutant, sink::error);
        this.sink = sink;
        this.capability = adjutant.capability();
        this.resultSetReader = createResultSetReader();

    }



    /*################################## blow StmtTask method ##################################*/


    @Override
    public final boolean isCancelled() {
        final boolean isCanceled;
        if (this.downstreamCanceled || this.hasError() || this.isTimeout()) {
            isCanceled = true;
        } else if (this.sink.isCancelled()) {
            log.trace("Downstream cancel subscribe.");
            this.downstreamCanceled = isCanceled = true;
        } else {
            isCanceled = false;
        }
        return isCanceled;
    }

    @Override
    public final void next(final ResultItem result) {
        this.sink.next(result);
    }

    @Override
    public final void addErrorToTask(Throwable error) {
        final TimeoutTask timeoutTask = this.timeoutTask;
        if (error instanceof UnrecognizedCollationException) {
            if (!containsError(UnrecognizedCollationException.class)) {
                addError(error);
            }
        } else if (timeoutTask == null
                || !(error instanceof MySQLServerException)
                || !MySQLStates.ER_QUERY_INTERRUPTED.equals(((MySQLServerException) error).getSqlState())) {
            addError(error);
        } else switch (timeoutTask.currentStatus()) {
            case RUNNING:
            case NORMAL_END:
            case CANCELED_AND_END:
                replaceErrorIfPresent(MySQLExceptions.statementTimeout(timeoutTask, getStmtTimeout(), error));
                break;
            case CANCELED:
                addError(error);
                break;
            case NONE:
            default:
                timeoutTask.cancel();
                addError(error);

        } //  else switch

    }

    @Override
    public final void addBigColumnPath(Path path) {
        List<Path> bigColumnPathList = this.bigColumnPathList;
        if (bigColumnPathList == null) {
            this.bigColumnPathList = bigColumnPathList = MySQLCollections.linkedList();
        }
        bigColumnPathList.add(path);
    }


    @Override
    public final TaskAdjutant adjutant() {
        return this.adjutant;
    }

    @Override
    public final void updateSequenceId(final int sequenceId) {
        if (sequenceId <= 0) {
            this.sequenceId = 0;
        } else {
            this.sequenceId = sequenceId & 0xFF;
        }
    }


    public final int nextSequenceId() {
        int sequenceId = this.sequenceId++;
        if (sequenceId > 0xFF) {
            sequenceId &= 0xFF;
            this.sequenceId = sequenceId;
        }
        return sequenceId;
    }

    @Override
    public final int nextResultNo() {
        return ++this.resultNo;
    }


    abstract void handleReadResultSetEnd();

    abstract ResultSetReader createResultSetReader();


    /**
     * @return true : send failure,task end.
     * @see #hasMoreBatchGroup()
     */
    abstract boolean executeNextGroup();

    abstract boolean executeNextFetch();


    final void readErrorPacket(final ByteBuf cumulateBuffer) {
        final int payloadLength;
        payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer)); //  sequence_id

        final MySQLServerException error;
        error = MySQLServerException.read(cumulateBuffer, payloadLength, this.capability, this.adjutant.errorCharset());
        addErrorToTask(error);
    }


    /**
     * @return true:task end
     */
    final boolean readResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {

        final ResultSetReader.States states;
        states = this.resultSetReader.read(cumulateBuffer, serverStatusConsumer);
        final boolean taskEnd;
        switch (states) {
            case END_ON_ERROR:
                taskEnd = true;
                break;
            case MORE_FETCH: {
                handleReadResultSetEnd();
                if (this.isCancelled()) {
                    taskEnd = true;
                } else {
                    taskEnd = executeNextFetch();
                }
            }
            break;
            case MORE_CUMULATE:
                taskEnd = false;
                break;
            case NO_MORE_RESULT: {
                handleReadResultSetEnd();
                if (hasMoreBatchGroup()) {
                    taskEnd = executeNextGroup();
                } else {
                    taskEnd = true;
                }
            }
            break;
            case MORE_RESULT: {
                handleReadResultSetEnd();
                taskEnd = false;
            }
            break;
            default:
                throw MySQLExceptions.unexpectedEnum(states);

        }
        return taskEnd;
    }


    /**
     * @return true: task end.
     */
    final boolean readUpdateResult(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {

        final int payloadLength;
        payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));

        final OkPacket ok;
        ok = OkPacket.readCumulate(cumulateBuffer, payloadLength, this.capability);
        serverStatusConsumer.accept(ok);

        final int resultNo = nextResultNo(); // must increment result no.

        final boolean taskEnd;
        if (this.isCancelled()) {
            taskEnd = !ok.hasMoreResult();
        } else {
            // emit update result.
            final boolean moreBatchGroup;
            if (isBatchStmt()) {
                moreBatchGroup = hasMoreBatchGroup();
                this.sink.next(MySQLResultStates.forBatchUpdate(resultNo, ok, moreBatchGroup));
            } else {
                moreBatchGroup = false;
                this.sink.next(MySQLResultStates.fromUpdate(resultNo, ok));
            }

            if (ok.hasMoreResult()) {
                taskEnd = false;
            } else if (moreBatchGroup) {
                taskEnd = executeNextGroup();
            } else {
                taskEnd = true;
            }

        }
        return taskEnd;
    }


    final void deleteBigColumnFileIfNeed() {
        final List<Path> bigColumnPathList = this.bigColumnPathList;
        if (bigColumnPathList == null) {
            return;
        }

        for (Path path : bigColumnPathList) {
            deleteBigColumnFile(path);
        }

        bigColumnPathList.clear();
        this.bigColumnPathList = null;

    }


    final void cancelTimeoutTaskIfNeed() {
        final TimeoutTask timeoutTask = this.timeoutTask;
        if (timeoutTask != null) {
            timeoutTask.cancel();
        }
    }

    /**
     * @return true : timeout
     */
    final boolean suspendTimeoutTaskIfNeed() {
        final TimeoutTask timeoutTask = this.timeoutTask;
        final boolean timeout;
        timeout = timeoutTask != null && timeoutTask.suspend();
        if (timeout && !containsError(TimeoutException.class)) {
            addError(MySQLExceptions.statementTimeout(timeoutTask, getStmtTimeout(), null));
        }
        return timeout;
    }


    final void resumeTimeoutTaskIfNeed() {
        final TimeoutTask timeoutTask = this.timeoutTask;
        final boolean timeout;
        timeout = timeoutTask != null && timeoutTask.resume();
        if (timeout && !containsError(TimeoutException.class)) {
            addError(MySQLExceptions.statementTimeout(timeoutTask, getStmtTimeout(), null));
        }
    }


    final boolean isTimeout() {
        final TimeoutTask timeoutTask = this.timeoutTask;
        final boolean timeout;
        timeout = timeoutTask != null && timeoutTask.isTimeout();
        if (timeout && !containsError(TimeoutException.class)) {
            addError(MySQLExceptions.statementTimeout(timeoutTask, getStmtTimeout(), null));
        }
        return timeout;
    }


    /**
     * @return true : timeout
     */
    final boolean startTimeoutTaskIfNeed() {
        final int timeoutMills;
        timeoutMills = getStmtTimeout();
        final boolean timeout;
        if (timeoutMills > 0 && this.timeoutTask == null) {
            final TimeoutTask task;
            this.timeoutTask = task = this.adjutant.getFactory()
                    .startTimeoutTask(this.adjutant.handshake10().threadId, timeoutMills);
            timeout = task.isTimeout();

            if (timeout && !containsError(TimeoutException.class)) {
                addError(MySQLExceptions.statementTimeout(task, getStmtTimeout(), null));
            }
        } else {
            timeout = false;
        }

        return timeout;
    }


    abstract int getStmtTimeout();


    /*-------------------below private method-------------------*/

    private void deleteBigColumnFile(final Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (Throwable e) {
            // ignore error
        }
    }


}
