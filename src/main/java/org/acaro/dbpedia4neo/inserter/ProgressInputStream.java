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

import java.io.IOException;
import java.io.InputStream;

/**
 * Stream wrapper to display progress
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
