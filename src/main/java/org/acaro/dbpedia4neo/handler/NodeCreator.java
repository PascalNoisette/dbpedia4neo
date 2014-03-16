

package org.acaro.dbpedia4neo.handler;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NodeCreator extends TripleHandler  {

    private final HashMap<String, String> allowedNode = new HashMap<String, String>();
    
    public NodeCreator()
    {
        try {
            Properties prop = new Properties();
            prop.load(NodeCreator.class.getClassLoader().getResourceAsStream("filter.properties"));
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

    @Override
    void handleNodePropertyRead(String node, String predicate, String propertyName) {
    }

    @Override
    void handleRelationshipRead(String node, String predicate, String relatedNodeName) {
        if (allowedNode.containsKey(relatedNodeName) && allowedNode.get(relatedNodeName).equals(predicate)) {
            neo.createNode(node);
        }
    }
}