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
import org.acaro.dbpedia4neo.handler.NodeCreator;
import org.acaro.dbpedia4neo.handler.TripleHandler;
import org.acaro.dbpedia4neo.inserter.db.BatchGraph;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandler;

public class DBpediaLoader {
    
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
