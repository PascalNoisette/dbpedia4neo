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

import org.openrdf.rio.RDFHandler;

/**
 * Parser handler to create relationship and add properties to existing nodes
 */
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