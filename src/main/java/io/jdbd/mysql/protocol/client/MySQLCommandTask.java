package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.ResultItem;
import io.jdbd.vendor.result.ResultSink;
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
 * </p>
 *
 * @see ComQueryTask
 * @see ComPreparedTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
abstract class MySQLCommandTask extends MySQLTask implements StmtTask {

    final Logger log = LoggerFactory.getLogger(getClass());

    final ResultSink sink;

    final int capability;

    private final ResultSetReader resultSetReader;

    private int sequenceId = 0;

    private int resultIndex;

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
        if (this.downstreamCanceled || this.hasError()) {
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
        addError(error);
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
    public final int nextResultIndex() {
        return this.resultIndex++;
    }


    abstract void handleReadResultSetEnd();

    abstract ResultSetReader createResultSetReader();

    /**
     * @return true: will invoke {@link #executeNextGroup()}
     */
    abstract boolean hasMoreGroup();

    /**
     * @return true : send failure,task end.
     * @see #hasMoreGroup()
     */
    abstract boolean executeNextGroup();

    abstract boolean executeNextFetch();


    final void readErrorPacket(final ByteBuf cumulateBuffer) {
        final int payloadLength;
        payloadLength = Packets.readInt3(cumulateBuffer);
        updateSequenceId(Packets.readInt1AsInt(cumulateBuffer)); //  sequence_id
        final ErrorPacket error;
        error = ErrorPacket.readCumulate(cumulateBuffer, payloadLength, this.capability, this.adjutant.errorCharset());
        addError(MySQLExceptions.createErrorPacketException(error));
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
                if (hasMoreGroup()) {
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

        final int resultIndex = nextResultIndex(); // must increment result index.
        final boolean noMoreResult = !ok.hasMoreResult();

        final boolean taskEnd;
        if (this.isCancelled()) {
            taskEnd = noMoreResult;
        } else {
            // emit update result.
            this.sink.next(MySQLResultStates.fromUpdate(resultIndex, ok));
            if (noMoreResult && hasMoreGroup()) {
                taskEnd = executeNextGroup();
            } else {
                taskEnd = noMoreResult;
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


    /*-------------------below private method-------------------*/

    private void deleteBigColumnFile(final Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (Throwable e) {
            // ignore error
        }
    }


}
