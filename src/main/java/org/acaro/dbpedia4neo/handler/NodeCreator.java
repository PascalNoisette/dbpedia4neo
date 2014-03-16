

package org.acaro.dbpedia4neo.handler;

public class NodeCreator extends TripleHandler {

    @Override
    void handleNodePropertyRead(String node, String predicate, String propertyName) {
    }

    @Override
    void handleRelationshipRead(String node, String predicate, String relatedNodeName) {
        neo.createNode(node);
    }
}