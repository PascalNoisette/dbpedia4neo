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
package org.acaro.dbpedia4neo.handler;

import org.acaro.dbpedia4neo.inserter.db.BatchGraph;
import org.openrdf.rio.RDFHandlerException;

/**
 * Parser handler to create index on existing node label
 */
public class NodeIndexer extends TripleHandler {

    @Override
    public void endRDF() throws RDFHandlerException {
        indexNode();
        indexNodeProperties();
    }
    
    /**
     * Add index on uri for each label
     */
    public void indexNode() {
        for (String label : allowedNode.keySet()) {
            neo.createIndexOnLabel(label, BatchGraph.INTERNAL_ATTRIBUTE_NAME);
        }
    }

    /**
     * Add index on each indexable properties for each label
     */
    public void indexNodeProperties() {
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
