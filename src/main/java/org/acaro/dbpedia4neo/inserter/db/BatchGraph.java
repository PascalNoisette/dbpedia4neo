/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.acaro.dbpedia4neo.inserter.db;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.RelationshipType;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.FULLTEXT_CONFIG;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserters;

/**
 *
 * @author Pascal
 */
public class BatchGraph {

    private BatchInserter graph;
    private boolean batchDBStarted = false;
    private long nodeCount = 0;
    private final BatchInserterIndex fulltext;
    private final BatchInserterIndex exact;
    private final LuceneBatchInserterIndexProvider indexProvider;

    public BatchGraph(String graphName) {
        graph = BatchInserters.inserter(graphName);
        batchDBStarted = true;
        indexProvider = new LuceneBatchInserterIndexProvider(graph);
        fulltext = indexProvider.nodeIndex("name", FULLTEXT_CONFIG);
        exact = indexProvider.nodeIndex("article", EXACT_CONFIG);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if (batchDBStarted) {
                        System.out.println("Shutting down batch inserter...");
                        indexProvider.shutdown();
                        graph.shutdown();
                        batchDBStarted = false;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        //init the base space
    }

    public void shutdown() {
        if (batchDBStarted) {
            System.out.println("Shutting down batch inserter...");
            indexProvider.shutdown();
            graph.shutdown();
            batchDBStarted = false;
        }
    }

    public void addNodeProperty(String nodeName, String predicate, String propertyName) {
        long id = hash(nodeName);
        if (graph.nodeExists(id)) {
            graph.setNodeProperty(id, predicate, propertyName);
        }
    }

    public long createNode(String nodeName) {
        long id = hash(nodeName);
        if (!graph.nodeExists(id)) {
            _createNode(id, nodeName);
        }
        return id;
    }

    private void _createNode(long id, String nodeName) {
        Map<String, Object> properties = new HashMap();
        properties.put("name", nodeName);
        graph.createNode(id, properties);

        fulltext.add(id, properties);
        exact.add(id, properties);

        if ((nodeCount++) % 10000 == 0) {
            System.out.println(nodeCount + " nodes created.");
        }
    }

    public void addRelationship(String nodeName, String predicate, String relatedNodeName) {
        long id = hash(nodeName);
        if (graph.nodeExists(id)) {
            graph.createRelationship(
                    id,
                    createNode(relatedNodeName),
                    getRelationshipType(predicate),
                    null
            );
        }
    }

    private RelationshipType getRelationshipType(final String context) {
        return new RelationshipType() {
            @Override
            public String name() {
                return context;
            }
        };
    }

    private long hash(String string) {
        int code = string.hashCode();
        if (code < 0) {
            code = -1 * code;
        }
        return (long) code;
    }
}
