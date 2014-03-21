package org.acaro.dbpedia4neo.handler;

import org.openrdf.rio.RDFHandler;

public class NodeUpdater extends TripleHandler implements RDFHandler {
    
    @Override
    void handleNodePropertyRead(String node, String predicate, String propertyName) {
        boolean indexProperty = false;
        if (indexableAttribute.contains(predicate)) {
            indexProperty = true;
        }
        neo.addNodeProperty(node, predicate, propertyName, indexProperty);
    }

    @Override
    void handleRelationshipRead(String node, String predicate, String relatedNodeName) {
        neo.addRelationship(node, predicate, relatedNodeName);
    }
}