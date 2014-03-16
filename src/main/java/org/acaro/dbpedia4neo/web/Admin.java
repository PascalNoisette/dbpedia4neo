/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.acaro.dbpedia4neo.web;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.WrappingNeoServerBootstrapper;

/**
 *
 * @author Pascal
 */
public class Admin {

    public static void main(String[] args) throws Exception {
        final GraphDatabaseService rawGraph = new GraphDatabaseFactory().newEmbeddedDatabase("dbpedia4neo");
        final WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper((GraphDatabaseAPI) rawGraph);

        // we need a clean shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    System.out.println("Shutting down...");
                    rawGraph.shutdown();
                    srv.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        srv.start();
        System.in.read();
        System.out.println("Shutting down...");
        rawGraph.shutdown();
        srv.stop();
    }
}
