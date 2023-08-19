package io.jdbd.mysql.protocol.client;


import io.jdbd.JdbdException;
import io.jdbd.result.CurrentRow;
import io.netty.buffer.ByteBuf;


abstract class MySQLCurrentRow extends MySQLRow.JdbdCurrentRow implements CurrentRow {

    final StmtTask task;

    private MySQLCurrentRow(MySQLRowMeta rowMeta, StmtTask task) {
        super(rowMeta);
        this.task = task;
    }

    @Override
    public boolean isBigRow() {
        return false;
    }


    abstract ResultSetReader.States readRows(ByteBuf cumulateBuffer) throws JdbdException;


    private static final class BinaryCurrentRow extends MySQLCurrentRow {

        private BinaryCurrentRow(MySQLRowMeta rowMeta, StmtTask task) {
            super(rowMeta, task);
        }


        @Override
        ResultSetReader.States readRows(final ByteBuf cumulateBuffer) throws JdbdException {
            return null;
        }

    }// BinaryCurrentRow


}
