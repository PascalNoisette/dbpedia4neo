package org.acaro.dbpedia4neo.handler;

import org.openrdf.rio.RDFHandler;

public class NodeUpdater extends TripleHandler implements RDFHandler {

    @Override
    void handleNodePropertyRead(String node, String predicate, String propertyName) {
        neo.addNodeProperty(node, predicate, propertyName);
    }

    @Override
    void handleRelationshipRead(String node, String predicate, String relatedNodeName) {
        neo.addRelationship(node, predicate, relatedNodeName);
    }
}