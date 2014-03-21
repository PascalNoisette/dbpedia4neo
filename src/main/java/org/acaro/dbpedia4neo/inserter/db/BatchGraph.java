/**
 *   This file is part of `PascalNoisette/dbpedia4neo` project
 *   Copyright (C) 2014  <netpascal0123@aol.com>
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.acaro.dbpedia4neo.inserter.db;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.FULLTEXT_CONFIG;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserters;

/**
 * Wrapper for BatchInserter
 */
public class BatchGraph {

    /**
     * Underlying batch graph
     */
    private BatchInserter graph;
    
    /**
     * Mark the graph as started to ease shutdown
     */
    private boolean batchDBStarted = false;
    
    /**
     * Created node counter
     */
    private long nodeCount = 0;
    
    /**
     * Underlying index for "name"
     */
    private final BatchInserterIndex fulltext;
    
    /**
     * Underlying index for "uri"
     */
    private final BatchInserterIndex exact;
    
    /**
     * Underlying index provider
     */
    private final LuceneBatchInserterIndexProvider indexProvider;
    
    /**
     * Flag for node to be created
     */
    public long  NODE_DOES_NOT_EXISTS = -1;
    
    /**
     * Property name for wiki page uri
     */
    public static String INTERNAL_ATTRIBUTE_NAME = "uri";
    
    /**
     * Cache for Dynamic label
     */
    private final Map<String, Label[]> labelList;

    /**
     * Start the underlying graph and indexes
     */
    public BatchGraph(String graphName) {
        graph = BatchInserters.inserter(graphName);
        batchDBStarted = true;
        indexProvider = new LuceneBatchInserterIndexProvider(graph);
        fulltext = indexProvider.nodeIndex("fulltext", FULLTEXT_CONFIG);
        exact = indexProvider.nodeIndex(INTERNAL_ATTRIBUTE_NAME, EXACT_CONFIG);
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

    /**
     * Stop the underlying graph and indexes
     */
    public void shutdown() {
        if (batchDBStarted) {
            System.out.println("Shutting down batch inserter...");
            indexProvider.shutdown();
            graph.shutdown();
            batchDBStarted = false;
        }
    }

    /**
     * Add a property to node
     * 
     * @param nodeName node name
     * @param propertyName property name
     * @param propertyValue property value
     * @param toIndex fulltext indexable
     */
    public void addNodeProperty(String nodeName, String propertyName, String propertyValue, boolean toIndex) {
        long node = getNode(nodeName);
        if (node != NODE_DOES_NOT_EXISTS && nodeHasALabel(node)) {
            if (graph.nodeHasProperty(node, propertyName)) {
                return;
            }
            graph.setNodeProperty(node, propertyName, propertyValue);
            if (toIndex) {
                fulltext.add(node, MapUtil.map(propertyName, propertyValue));
            }
        }
    }

    /**
     * Create node without label
     * 
     * @param nodeName node name
     * 
     * @return node id
     */
    public long createNode(String nodeName) 
    {
        return createNode(nodeName, null);
    }
    
    /**
     * Create node if not exists
     * 
     * @param nodeName node name
     * @param label label name or null
     * 
     * @return node id
     */
    public long createNode(String nodeName, String label) {
        long node = getNode(nodeName);
        if (node == NODE_DOES_NOT_EXISTS) {
            return _createNode(nodeName, getLabel(label));
        }
        return getNode(nodeName);
    }

    /**
     * Create a node
     * 
     * @param nodeName node name
     * @param label array of Dynamiclabel or null
     * 
     * @return node id
     */
    private long _createNode(String nodeName, Label[] label) {
        Map<String, Object> properties = new HashMap();
        properties.put(INTERNAL_ATTRIBUTE_NAME, nodeName);
        long id;
        if (label != null) {
            id = graph.createNode(properties, label);
        } else {
            id = graph.createNode(properties);
        }
        exact.add(id, properties);

        if ((nodeCount++) % 10000 == 0) {
            System.out.println(nodeCount + " nodes created.");
        }
        return id;
    }

    /**
     * Create a relationship between nodeName and relatedNodeName
     * 
     * @param nodeName node name
     * @param relationType relation type
     * @param relatedNodeName related node name
     */
    public void addRelationship(String nodeName, String relationType, String relatedNodeName) {
        long node = getNode(nodeName);
        if (node != NODE_DOES_NOT_EXISTS && nodeHasALabel(node)) {
            graph.createRelationship(
                    node,
                    createNode(relatedNodeName),
                    getRelationshipType(relationType),
                    null
            );
        }
    }

    /**
     * Create a dynamic relationship type
     * 
     * @param context relationship type name
     */
    private RelationshipType getRelationshipType(final String context) {
        return new RelationshipType() {
            @Override
            public String name() {
                return context;
            }
        };
    }

    /**
     * Use batch index to retrive node by uri
     * 
     * @param nodeName nodename
     * 
     * @return node id
     */
    private long getNode(String nodeName) {
        exact.flush();
        Long node = exact.get(INTERNAL_ATTRIBUTE_NAME, nodeName).getSingle();
        if (node==null) {
            return NODE_DOES_NOT_EXISTS;
        }
        return node;
    }
    
    /**
     * Retrive the dynamic label by name
     * 
     * @param label label name
     * 
     * @return one Dynamic Label in a array
     */
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
    
    /**
     * Create the dynamic label
     * 
     * @param name label name
     * 
     * @return one Dynamic Label in a array
     */
    public Label[] _createLabel(String name) {
        Label personLabel = DynamicLabel.label(name);
        Label[] value = new Label[1];
        value[0] = personLabel;
        return value;
    }
    
    /**
     * Remove RDF related information from label name
     * 
     * @param name label name
     * 
     * @return string
     */
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
    
    /**
     * Create index on label
     * 
     * @param labelName label name
     * @param propertyName property to index
     */
    public void createIndexOnLabel(String labelName, String propertyName)
    {
        graph.createDeferredSchemaIndex(getLabel(labelName)[0]).on(propertyName).create();
    }

    
    /**
     * Check if node belong to at least one label
     * 
     * @param node id
     * 
     * @return boolean
     */
    private boolean nodeHasALabel(long node) {
        Iterable<Label> labels = graph.getNodeLabels(node);
        while (labels.iterator().hasNext()) {
            return true;
        }
        return false;
    }
}
