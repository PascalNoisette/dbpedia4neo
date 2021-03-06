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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.openrdf.rio.ParseErrorListener;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.ntriples.NTriplesParser;

import java.io.InputStream;
import org.acaro.dbpedia4neo.handler.TripleHandler;
import org.acaro.dbpedia4neo.inserter.db.BatchGraph;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandler;

/**
 * Run any given parser handler against the input file
 */
public class DBpediaLoader {
    
    /**
     * Run the given parser handler against each file in argument
     * 
     * @param args file list
     * @param tripleHandler parser handler
     * 
     * @throws RDFParseException
     * @throws RDFHandlerException
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public void updateNodes(String[] args, TripleHandler tripleHandler) throws RDFParseException, RDFHandlerException, FileNotFoundException, IOException {
        BatchGraph neo = new BatchGraph("dbpedia4neo");
        tripleHandler.setGraph(neo);
        for (String file : args) {
            System.out.println("Loading " + file + ": updateNodes");
            loadFile(file, tripleHandler);
            System.out.print('\n');
        }
        neo.shutdown();
    }

    /**
     * Run the given parser handler against a file
     * 
     * @param file
     * @param handler
     * 
     * @throws RDFParseException
     * @throws RDFHandlerException
     * @throws FileNotFoundException
     * @throws IOException 
     */
    private static void loadFile(final String file, RDFHandler handler) throws RDFParseException, RDFHandlerException, FileNotFoundException, IOException {
        NTriplesParser parser = new NTriplesParser(new ValueFactoryImpl());
        parser.setRDFHandler(handler);
        parser.setParseErrorListener(new ParseErrorListener() {

            @Override
            public void warning(String msg, int lineNo, int colNo) {
                System.err.println("warning: " + msg);
				System.err.println("file: " + file + " line: " + lineNo + " column: " +colNo);
            }

            @Override
            public void error(String msg, int lineNo, int colNo) {
                System.err.println("error: " + msg);
				System.err.println("file: " + file + " line: " + lineNo + " column: " +colNo);
            }

            @Override
            public void fatalError(String msg, int lineNo, int colNo) {
                System.err.println("fatal: " + msg);
				System.err.println("file: " + file + " line: " + lineNo + " column: " +colNo);
            }

        });

        InputStream stream = openStream(file);
        parser.parse(stream, "http://dbpedia.org/");
        stream.close();
    }

    /**
     * Open input bzipped file
     *
     * @param fileName file to import or `-` to read from stdin
     *
     * @return stream
     *
     * @throws IOException
     */
    protected static InputStream openStream(String fileName) throws IOException {
        InputStream stream = System.in;
        if (!"-".equals(fileName)) {//read from stdin if file argument is the `-` symbol
            stream = new FileInputStream(fileName);
        }
        stream = new BufferedInputStream(stream);
        return new ProgressInputStream(stream);
    }
}
