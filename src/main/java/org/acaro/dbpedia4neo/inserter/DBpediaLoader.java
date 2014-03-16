package org.acaro.dbpedia4neo.inserter;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.openrdf.model.ValueFactory;
import org.openrdf.rio.ParseErrorListener;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.tinkerpop.blueprints.impls.neo4j.batch.SailableNeo4jBatchGraph;
import com.tinkerpop.blueprints.oupls.sail.GraphSail;
import java.io.InputStream;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class DBpediaLoader 
{
    public static void main( String[] args ) 
    	throws SailException, RDFParseException, RDFHandlerException, FileNotFoundException, IOException
    {
        
        final BatchInserter graph = BatchInserters.inserter("dbpedia4neo");
    	//https://groups.google.com/forum/#!msg/neo4j/g8bV8w3LH9E/tMaEWGzGBMMJ
        //"No solution out of the box so" ... so this little trick
        SailableNeo4jBatchGraph neo = new SailableNeo4jBatchGraph(graph, new LuceneBatchInserterIndexProvider(graph));
        Sail sail = new GraphSail(neo);
        sail.initialize();
    	for (String file: args) {
    		System.out.println("Loading " + file + ": ");
    		loadFile(file, sail.getConnection(), sail.getValueFactory());
    		System.out.print('\n');
    	}
    	sail.shutDown();
    }

	private static void loadFile(final String file, SailConnection sc, ValueFactory vf) throws RDFParseException, RDFHandlerException, FileNotFoundException, IOException {
		NTriplesParser parser = new NTriplesParser(vf);
		TripleHandler handler = new TripleHandler(sc);
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
