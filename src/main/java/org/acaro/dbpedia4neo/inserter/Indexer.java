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