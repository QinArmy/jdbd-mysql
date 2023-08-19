package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.stmt.ParamSingleStmt;

interface PrepareStmtTask {

    ParamSingleStmt getStmt();

    /**
     * @throws IllegalStateException throw when before prepare.
     */
    int getStatementId();

    /**
     * @throws IllegalStateException throw when before prepare.
     */
    MySQLColumnMeta[] getParameterMetas();

    TaskAdjutant adjutant();

    int nextSequenceId();

   void resetSequenceId();

    void addErrorToTask(Throwable error);

    boolean isSupportFetch();

    void nextGroupReset();

    void handleExecuteMessageError();


}
