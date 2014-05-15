package org.neo4j.unsafe.impl.batchimport;

import java.util.Map;

public interface LineData
{

    class Header
    {
        public Header( int column, String name, Type type, String indexName )
        {
            this.column = column;
            this.name = name;
            this.type = type;
            this.indexName = indexName;
        }

        public final int column;
        public final String name;
        public final Type type;
        public final String indexName; // todo index config in config

        @Override
        public String toString()
        {
            return column + ". " + name +
                    (type != null ? " type: " + type : "") +
                    (indexName != null ? " index: " + indexName : "");
        }
    }

    boolean processLine( String line );

    Header[] getHeader();

    long getId();

    Map<String, Object> getProperties();

    Map<String, Map<String, Object>> getIndexData();

    String[] getTypeLabels();

    String getRelationshipTypeLabel();

    Object getValue( int column );

    boolean hasId();

    Object[] getLineData();
}
