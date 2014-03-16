
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.acaro.dbpedia4neo.inserter;

import org.acaro.dbpedia4neo.handler.NodeUpdater;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

/**
 *
 * @author Pascal
 */
public class Updater {

    public static void main(String[] args)
            throws RDFParseException, RDFHandlerException, FileNotFoundException, IOException {
        (new DBpediaLoader()).updateNodes(args, new NodeUpdater());
    }
}