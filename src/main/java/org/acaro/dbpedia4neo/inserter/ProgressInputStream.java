/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.acaro.dbpedia4neo.inserter;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Pascal
 */
public class ProgressInputStream extends InputStream {

    InputStream underlyingStream = null;
    private int lineCounter = 0;
    
    public ProgressInputStream(InputStream s)
    {
        underlyingStream = s;
    }

    @Override
    public int read() throws IOException {
        int c = underlyingStream.read();
        if (c=='\n') {
            lineRead();
        }
        return c;
    }
    
    @Override
    public int read(byte b[]) throws IOException {
        int c = underlyingStream.read(b);
        for (int i=0;i<c;i++){
            if (b[i]=='\n') {
                lineRead();
            }
        }
        return c;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int c = underlyingStream.read(b, off, len);
        for (int i=off;i<off+c;i++) {
            if (b[i]=='\n') {
                lineRead();
            }
        }
        return c;
    }

    @Override
    public long skip(long n) throws IOException {
        return underlyingStream.skip(n);
    }

    @Override
    public int available() throws IOException {
        return underlyingStream.available();
    }
    
    @Override
    public void close() throws IOException {
        underlyingStream.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        underlyingStream.mark(readlimit);
    }    
    @Override
    public synchronized void reset() throws IOException {
        underlyingStream.reset();
    }
    @Override
    public boolean markSupported() {
        return underlyingStream.markSupported();
    }

    private void lineRead() {
        lineCounter++;
        if (lineCounter%10000==0) {
            System.out.println(lineCounter + " lines processed.");
        }
    }
    
}
