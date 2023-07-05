package io.jdbd.meta;

import java.util.Collections;
import java.util.List;

public final class TableIndexMetaData {

    private final DatabaseTableMetaData tableMeta;

    private final String indexName;

    private final String indexType;

    private final boolean unique;

    private final List<IndexColumnMeta> indexColumnList;

    public TableIndexMetaData(DatabaseTableMetaData tableMeta, String indexName, String indexType
            , boolean unique, List<IndexColumnMeta> indexColumnList) {
        this.tableMeta = tableMeta;
        this.indexName = indexName;
        this.indexType = indexType;
        this.unique = unique;
        this.indexColumnList = Collections.unmodifiableList(indexColumnList);
    }

    public DatabaseTableMetaData getTableMeta() {
        return this.tableMeta;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public String getIndexType() {
        return this.indexType;
    }

    public boolean isUnique() {
        return this.unique;
    }

    public List<IndexColumnMeta> getIndexColumnList() {
        return this.indexColumnList;
    }
}