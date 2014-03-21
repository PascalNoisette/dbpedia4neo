/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.acaro.dbpedia4neo.handler;

import org.acaro.dbpedia4neo.inserter.db.BatchGraph;
import org.openrdf.rio.RDFHandlerException;

/**
 *
 * @author Pascal
 */
public class NodeIndexer extends TripleHandler {

    @Override
    public void endRDF() throws RDFHandlerException {
        indexNode();
        indexNodeProperties();
    }
    
    public void indexNode() throws RDFHandlerException {
        for (String label : allowedNode.keySet()) {
            neo.createIndexOnLabel(label, BatchGraph.INTERNAL_ATTRIBUTE_NAME);
        }
    }

    
    public void indexNodeProperties() throws RDFHandlerException {
        for (String label : allowedNode.keySet()) {
            for (String property : indexableAttribute) {
                neo.createIndexOnLabel(label, property);
            }
        }
    }

    @Override
    void handleNodePropertyRead(String node, String predicate, String propertyName) {
    }

    @Override
    void handleRelationshipRead(String node, String predicate, String relatedNodeName) {
    }
    
}
