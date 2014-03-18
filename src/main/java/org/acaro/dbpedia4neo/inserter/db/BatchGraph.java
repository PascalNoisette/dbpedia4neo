/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.acaro.dbpedia4neo.inserter.db;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
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
    public long  NODE_DOES_NOT_EXISTS = -1;
    private final Map<String, Label[]> labelList;

    public BatchGraph(String graphName) {
        graph = BatchInserters.inserter(graphName);
        batchDBStarted = true;
        indexProvider = new LuceneBatchInserterIndexProvider(graph);
        fulltext = indexProvider.nodeIndex("name", FULLTEXT_CONFIG);
        exact = indexProvider.nodeIndex("article", EXACT_CONFIG);
        labelList = new HashMap<String, Label[]>();
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
        long node = getNode(nodeName);
        if (node != NODE_DOES_NOT_EXISTS && nodeHasALabel(node)) {
            graph.setNodeProperty(node, predicate, propertyName);
        }
    }

    public long createNode(String nodeName) 
    {
        return createNode(nodeName, null);
    }
    public long createNode(String nodeName, String label) {
        long node = getNode(nodeName);
        if (node == NODE_DOES_NOT_EXISTS) {
            return _createNode(nodeName, getLabel(label));
        }
        return getNode(nodeName);
    }

    private long _createNode(String nodeName, Label[] label) {
        Map<String, Object> properties = new HashMap();
        properties.put("name", nodeName);
        long id;
        if (label != null) {
            id = graph.createNode(properties, label);
        } else {
            id = graph.createNode(properties);
        }
        fulltext.add(id, properties);
        exact.add(id, properties);

        if ((nodeCount++) % 10000 == 0) {
            System.out.println(nodeCount + " nodes created.");
        }
        return id;
    }

    public void addRelationship(String nodeName, String predicate, String relatedNodeName) {
        long node = getNode(nodeName);
        if (node != NODE_DOES_NOT_EXISTS && nodeHasALabel(node)) {
            graph.createRelationship(
                    node,
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

    private long getNode(String nodeName) {
        exact.flush();
        Long node = exact.get("name", nodeName).getSingle();
        if (node==null) {
            return NODE_DOES_NOT_EXISTS;
        }
        return node;
    }
    
    
    public Label[] getLabel(String label) {
        if (label == null) {
            return null;
        }
        if (!labelList.containsKey(label)) {
            String name = cleanLabelName(label);
            labelList.put(label, _createLabel(name));
        }
        return labelList.get(label);
    }
    
    public Label[] _createLabel(String name) {
        Label personLabel = DynamicLabel.label(name);
        Label[] value = new Label[1];
        value[0] = personLabel;
        return value;
    }
    
    private String cleanLabelName(String name) {
        String[] names = name.split(":");
        if (names.length>=2) {
            name=names[1];
        }
        names = name.split("Infobox_");
        if (names.length>=2) {
            name=names[1];
        }
        return name;
    }
    
    public void createIndexOnLabel(String name)
    {
        graph.createDeferredSchemaIndex(getLabel(name)[0]).on("name").create();
    }

    private boolean nodeHasALabel(long node) {
        Iterable<Label> labels = graph.getNodeLabels(node);
        while (labels.iterator().hasNext()) {
            return true;
        }
        return false;
    }
}
