
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.acaro.dbpedia4neo.inserter;

import org.acaro.dbpedia4neo.handler.NodeIndexer;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.acaro.dbpedia4neo.inserter.db.BatchGraph;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

/**
 *
 * @author Pascal
 */
public class Indexer {

    public static void main(String[] args)
            throws RDFParseException, RDFHandlerException, FileNotFoundException, IOException {
        BatchGraph neo = new BatchGraph("dbpedia4neo");
        NodeIndexer worker = new NodeIndexer();
        worker.setGraph(neo);
        worker.endRDF();
        neo.shutdown();
    }
}