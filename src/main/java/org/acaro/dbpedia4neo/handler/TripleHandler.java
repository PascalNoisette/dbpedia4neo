
package org.acaro.dbpedia4neo.handler;

import org.acaro.dbpedia4neo.inserter.db.BatchGraph;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

abstract public class TripleHandler implements RDFHandler {

    protected BatchGraph neo;

    public void setGraph(BatchGraph neo) {
        this.neo = neo;
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

    public void startRDF() throws RDFHandlerException {
    }

    public void endRDF() throws RDFHandlerException {
    }

    abstract void handleNodePropertyRead(String node, String predicate, String propertyName);

    abstract void handleRelationshipRead(String node, String predicate, String relatedNodeName);
}