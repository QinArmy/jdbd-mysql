package io.jdbd.mysql.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.result.ResultStates;
import io.jdbd.result.Warning;
import io.jdbd.session.Option;

abstract class MySQLResultStates implements ResultStates {

    static MySQLResultStates fromUpdate(final int resultNo, final Terminator terminator) {
        return new UpdateResultStates(resultNo, terminator);
    }

    static MySQLResultStates fromQuery(final int resultIndex, final Terminator terminator, final long rowCount) {
        return new QueryResultStates(resultIndex, terminator, rowCount);
    }


    private static final Option<Boolean> SERVER_MORE_QUERY_EXISTS = Option.from("SERVER_MORE_QUERY_EXISTS", Boolean.class);

    private static final Option<Boolean> SERVER_MORE_RESULTS_EXISTS = Option.from("SERVER_MORE_RESULTS_EXISTS", Boolean.class);


    private static final Option<Boolean> SERVER_QUERY_NO_GOOD_INDEX_USED = Option.from("SERVER_QUERY_NO_GOOD_INDEX_USED", Boolean.class);

    private static final Option<Boolean> SERVER_QUERY_NO_INDEX_USED = Option.from("SERVER_QUERY_NO_INDEX_USED", Boolean.class);

    private static final Option<Boolean> SERVER_STATUS_CURSOR_EXISTS = Option.from("SERVER_STATUS_CURSOR_EXISTS", Boolean.class);

    private static final Option<Boolean> SERVER_STATUS_LAST_ROW_SENT = Option.from("SERVER_STATUS_LAST_ROW_SENT", Boolean.class);

    private static final Option<Boolean> SERVER_STATUS_DB_DROPPED = Option.from("SERVER_STATUS_DB_DROPPED", Boolean.class);

    private static final Option<Boolean> SERVER_STATUS_METADATA_CHANGED = Option.from("SERVER_STATUS_METADATA_CHANGED", Boolean.class);

    private static final Option<Boolean> SERVER_QUERY_WAS_SLOW = Option.from("SERVER_QUERY_WAS_SLOW", Boolean.class);

    private static final Option<Boolean> SERVER_PS_OUT_PARAMS = Option.from("SERVER_PS_OUT_PARAMS", Boolean.class);

    private static final Option<Boolean> SERVER_SESSION_STATE_CHANGED = Option.from("SERVER_SESSION_STATE_CHANGED", Boolean.class);


    private final int resultNo;

    final int serverStatus;

    private final long affectedRows;

    private final long insertedId;

    private final String message;

    private final Warning warning;


    private MySQLResultStates(final int resultNo, final Terminator terminator) {
        this.resultNo = resultNo;
        if (terminator instanceof OkPacket) {
            final OkPacket ok = (OkPacket) terminator;
            this.serverStatus = ok.getStatusFags();
            this.affectedRows = ok.getAffectedRows();
            this.insertedId = ok.getLastInsertId();
            this.message = ok.getInfo();

            final int count;
            count = ok.getWarnings();
            if (count > 0) {
                this.warning = new WarningCount(count);
            } else {
                this.warning = null;
            }
        } else if (terminator instanceof EofPacket) {
            final EofPacket eof = (EofPacket) terminator;

            this.serverStatus = eof.getStatusFags();
            this.affectedRows = 0L;
            this.insertedId = 0L;
            this.message = "";
            this.warning = null;
        } else {
            throw new IllegalArgumentException(String.format("terminator isn't %s or %s",
                    OkPacket.class.getName(), EofPacket.class.getName()));
        }
    }


    @Override
    public final int getResultNo() {
        return this.resultNo;
    }

    @Override
    public final boolean isSupportInsertId() {
        return true;
    }

    @Override
    public final boolean inTransaction() {
        return Terminator.inTransaction(this.serverStatus);
    }

    @Override
    public final long affectedRows() {
        return this.affectedRows;
    }

    @Override
    public final long lastInsertedId() {
        return this.insertedId;
    }

    @Override
    public final String message() {
        return this.message;
    }


    @Override
    public final boolean hasMoreResult() {
        return (this.serverStatus & Terminator.SERVER_MORE_RESULTS_EXISTS) != 0;
    }

    @Override
    public final boolean hasMoreFetch() {
        final int serverStatus = this.serverStatus;
        return (serverStatus & Terminator.SERVER_STATUS_CURSOR_EXISTS) != 0
                && (serverStatus & Terminator.SERVER_STATUS_LAST_ROW_SENT) == 0;
    }


    /**
     * <p>
     * jdbd-mysql support following :
     *     <ul>
     *         <li>{@link Option#AUTO_COMMIT}</li>
     *         <li>{@link Option#IN_TRANSACTION}</li>
     *         <li>{@link Option#READ_ONLY}</li>
     *         <li>{@link #SERVER_MORE_QUERY_EXISTS}</li>
     *         <li>{@link #SERVER_MORE_RESULTS_EXISTS}</li>
     *         <li>{@link #SERVER_QUERY_NO_GOOD_INDEX_USED}</li>
     *         <li>{@link #SERVER_QUERY_NO_INDEX_USED}</li>
     *         <li>{@link #SERVER_STATUS_CURSOR_EXISTS}</li>
     *         <li>{@link #SERVER_STATUS_LAST_ROW_SENT}</li>
     *         <li>{@link #SERVER_STATUS_DB_DROPPED}</li>
     *         <li>{@link #SERVER_STATUS_METADATA_CHANGED}</li>
     *         <li>{@link #SERVER_QUERY_WAS_SLOW}</li>
     *         <li>{@link #SERVER_PS_OUT_PARAMS}</li>
     *         <li>{@link #SERVER_SESSION_STATE_CHANGED}</li>
     *     </ul>
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a1d854e841086925be1883e4d7b4e8cad">SERVER_STATUS_flags_enum</a>
     */
    @SuppressWarnings("unchecked")
    @Override
    public final <T> T valueOf(final @Nullable Option<T> option) {
        final Boolean value;
        if (option == null) {
            value = null;
        } else if (option == Option.AUTO_COMMIT) {
            value = (this.serverStatus & Terminator.SERVER_STATUS_AUTOCOMMIT) != 0;
        } else if (option == Option.IN_TRANSACTION) {
            value = (this.serverStatus & Terminator.SERVER_STATUS_IN_TRANS) != 0;
        } else if (option == Option.READ_ONLY) {
            value = (this.serverStatus & Terminator.SERVER_STATUS_IN_TRANS_READONLY) != 0;
        } else if (option.equals(SERVER_MORE_QUERY_EXISTS)) {
            value = (this.serverStatus & Terminator.SERVER_MORE_QUERY_EXISTS) != 0;
        } else if (option.equals(SERVER_MORE_RESULTS_EXISTS)) {
            value = (this.serverStatus & Terminator.SERVER_MORE_RESULTS_EXISTS) != 0;
        } else if (option.equals(SERVER_QUERY_NO_GOOD_INDEX_USED)) {
            value = (this.serverStatus & Terminator.SERVER_QUERY_NO_GOOD_INDEX_USED) != 0;
        } else if (option.equals(SERVER_QUERY_NO_INDEX_USED)) {
            value = (this.serverStatus & Terminator.SERVER_QUERY_NO_INDEX_USED) != 0;
        } else if (option.equals(SERVER_STATUS_CURSOR_EXISTS)) {
            value = (this.serverStatus & Terminator.SERVER_STATUS_CURSOR_EXISTS) != 0;
        } else if (option.equals(SERVER_STATUS_LAST_ROW_SENT)) {
            value = (this.serverStatus & Terminator.SERVER_STATUS_LAST_ROW_SENT) != 0;
        } else if (option.equals(SERVER_STATUS_DB_DROPPED)) {
            value = (this.serverStatus & Terminator.SERVER_STATUS_DB_DROPPED) != 0;
        } else if (option.equals(SERVER_STATUS_METADATA_CHANGED)) {
            value = (this.serverStatus & Terminator.SERVER_STATUS_METADATA_CHANGED) != 0;
        } else if (option.equals(SERVER_QUERY_WAS_SLOW)) {
            value = (this.serverStatus & Terminator.SERVER_QUERY_WAS_SLOW) != 0;
        } else if (option.equals(SERVER_PS_OUT_PARAMS)) {
            value = (this.serverStatus & Terminator.SERVER_PS_OUT_PARAMS) != 0;
        } else if (option.equals(SERVER_SESSION_STATE_CHANGED)) {
            value = (this.serverStatus & Terminator.SERVER_SESSION_STATE_CHANGED) != 0;
        } else {
            value = null;
        }
        return (T) value;
    }

    @Override
    public final Warning warning() {
        return this.warning;
    }

    private static final class UpdateResultStates extends MySQLResultStates {

        private UpdateResultStates(int resultIndex, Terminator terminator) {
            super(resultIndex, terminator);
        }

        @Override
        public long rowCount() {
            return 0L;
        }

        @Override
        public boolean hasColumn() {
            return false;
        }

    }// UpdateResultStates

    private static final class QueryResultStates extends MySQLResultStates {

        private final long rowCount;

        private QueryResultStates(int resultNo, Terminator terminator, long rowCount) {
            super(resultNo, terminator);
            this.rowCount = rowCount;
        }

        @Override
        public long rowCount() {
            return this.rowCount;
        }

        @Override
        public boolean hasColumn() {
            return true;
        }

    }// QueryResultStates


    private static final class WarningCount implements Warning {

        private final int warningCount;

        private final String msg;

        private WarningCount(int warningCount) {
            assert warningCount > 0;
            this.warningCount = warningCount;
            this.msg = "warning count : " + warningCount;

        }

        @Override
        public String message() {
            return this.msg;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T valueOf(Option<T> option) {
            if (option != Option.WARNING_COUNT) {
                return null;
            }
            return (T) Integer.valueOf(this.warningCount);
        }

    }// WarningCount

}
