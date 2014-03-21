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

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.acaro.dbpedia4neo.inserter.db.BatchGraph;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * Parser handler abstract
 */
abstract public class TripleHandler implements RDFHandler {

    protected BatchGraph neo;
    protected HashMap<String, String> allowedNode = new HashMap<String, String>();
    HashSet<String> indexableAttribute;
    
    /**
     * Initialise allowed node to be created and indexable attribute list
     */
    TripleHandler ()
    {
        loadAllowedNodes();
        loadIndexableAttribute();
    }

    /**
     * Assign graph to work on
     * @param neo  Batch inserter wrapper
     */
    public void setGraph(BatchGraph neo) {
        this.neo = neo;
    }
    
    
    /**
     * Build allowed node list to be created based on the filter.properties file
     */
    private void loadAllowedNodes()
    {
        try {
            Properties prop = new Properties();
            prop.load(TripleHandler.class.getClassLoader().getResourceAsStream("filter.properties"));
            Enumeration enuKeys = prop.keys();
            while (enuKeys.hasMoreElements()) {
                    String key = (String) enuKeys.nextElement();
                    String value = prop.getProperty(key);
                    allowedNode.put(key, value);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(NodeCreator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Add name to the list of fulltext indexable attribute
     */
    private void loadIndexableAttribute()
    {
        indexableAttribute = new HashSet<String>();
        indexableAttribute.add("name");
    }

    @Override
    public void handleComment(String arg0) throws RDFHandlerException {
    }

    @Override
    public void handleNamespace(String arg0, String arg1)
            throws RDFHandlerException {
    }

    @Override
    public void handleStatement(Statement arg0) {

        try {
            // avoid self-cycles
            if (arg0.getSubject().stringValue().equals(arg0.getObject().stringValue())) {
                return;
            }
            if (!(arg0.getSubject() instanceof URI)) {
                return;
            }
            String node = ((URI) arg0.getSubject()).getLocalName();
            String predicate = arg0.getPredicate().getLocalName();

            if (arg0.getObject() instanceof URI && !((URI) arg0.getObject()).getLocalName().isEmpty()) {
                String relatedNodeName = ((URI) arg0.getObject()).getLocalName();
                handleRelationshipRead(node, predicate, relatedNodeName);
            } else {
                String propertyName = arg0.getObject().stringValue();
                handleNodePropertyRead(node, predicate, propertyName);
            }
        } catch (Exception e) {
            e.printStackTrace();
			System.out.println("Subject: " + arg0.getSubject().toString() +
					" Predicate: " + arg0.getPredicate().toString() +
					" Object: " + arg0.getObject().toString());
        }
    }

    @Override
    public void startRDF() throws RDFHandlerException {
    }

    @Override
    public void endRDF() throws RDFHandlerException {
    }

    /**
     * Convert a line of RDF file to a node, or a property
     */
    abstract void handleNodePropertyRead(String node, String predicate, String propertyName);

    /**
     * Convert a line of RDF file to a relationship
     */
    abstract void handleRelationshipRead(String node, String predicate, String relatedNodeName);
}