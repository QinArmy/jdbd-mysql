package io.jdbd.mysql.protocol.client;

import io.jdbd.result.ResultStatus;

abstract class MySQLResultStatus implements ResultStatus {

    static MySQLResultStatus from(TerminatorPacket terminator) {
        return new TerminalResultStatus(terminator);
    }

    private final int serverStatus;

    private final long affectedRows;

    private final long insertedId;

    private final int warnings;


    private MySQLResultStatus(final TerminatorPacket terminator) {
        if (terminator instanceof OkPacket) {
            OkPacket ok = (OkPacket) terminator;

            this.serverStatus = ok.getStatusFags();
            this.affectedRows = ok.getAffectedRows();
            this.insertedId = ok.getLastInsertId();
            this.warnings = ok.getWarnings();

            //  this.sqlState = ok.getSessionStateInfo();
        } else if (terminator instanceof EofPacket) {
            EofPacket eof = (EofPacket) terminator;

            this.serverStatus = eof.getStatusFags();
            this.affectedRows = 0L;
            this.insertedId = 0L;
            this.warnings = eof.getWarnings();
        } else {
            throw new IllegalArgumentException(String.format("terminator isn't %s or %s"
                    , OkPacket.class.getName(), EofPacket.class.getName()));
        }
    }


    public final int getServerStatus() {
        return this.serverStatus;
    }


    @Override
    public final long getAffectedRows() {
        return this.affectedRows;
    }

    @Override
    public final long getInsertId() {
        return this.insertedId;
    }

    @Override
    public final int getWarnings() {
        return this.warnings;
    }


    @Override
    public final boolean hasMoreResult() {
        return (this.serverStatus & ClientProtocol.SERVER_MORE_RESULTS_EXISTS) != 0;
    }

    @Override
    public final boolean hasMoreFetch() {
        final int serverStatus = this.serverStatus;
        return (serverStatus & ClientProtocol.SERVER_STATUS_CURSOR_EXISTS) != 0
                && (serverStatus & ClientProtocol.SERVER_STATUS_LAST_ROW_SENT) == 0;
    }

    private static final class TerminalResultStatus extends MySQLResultStatus {

        private TerminalResultStatus(TerminatorPacket terminator) {
            super(terminator);
        }

    }


}