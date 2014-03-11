package com.tinkerpop.blueprints.impls.neo4j.batch;

import com.tinkerpop.blueprints.Vertex;
import java.util.ArrayList;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;

public class SailableNeo4jBatchGraph extends Neo4jBatchGraph{
    
    protected LuceneBatchInserterIndexProvider hackAnAccessToLuceneRef = null;

    public SailableNeo4jBatchGraph(BatchInserter graph, LuceneBatchInserterIndexProvider luceneBatchInserterIndexProvider) {
         super(graph, luceneBatchInserterIndexProvider);
         hackAnAccessToLuceneRef = luceneBatchInserterIndexProvider;
    }
    
     /**
     * @throws UnsupportedOperationException
     */
    @Override
    public Iterable<Vertex> getVertices(final String key, final Object value) throws UnsupportedOperationException {
        return new ArrayList<Vertex>();
    }
    
    
    @Override
    public void shutdown() {
        this.flushIndices();
        this.hackAnAccessToLuceneRef.shutdown();
        getRawGraph().shutdown();
        
        //no way with my 25 000 000 triple :  removeReferenceNodeAndFinalizeKeyIndices();
    }
}
