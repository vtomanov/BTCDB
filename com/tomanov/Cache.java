package com.tomanov;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * USE OF THIS SOFTWARE IS GOVERNED BY THE TERMS AND CONDITIONS
 * OF THE LICENSE STATEMENT AND LIMITED WARRANTY FURNISHED WITH
 * THE PRODUCT.
 * <p/>
 * IN PARTICULAR, YOU WILL INDEMNIFY AND HOLD ITS AUTHOR, ITS
 * RELATED ENTITIES AND ITS SUPPLIERS, HARMLESS FROM AND AGAINST ANY
 * CLAIMS OR LIABILITIES ARISING OUT OF THE USE, REPRODUCTION, OR
 * DISTRIBUTION OF YOUR PROGRAMS, INCLUDING ANY CLAIMS OR LIABILITIES
 * ARISING OUT OF OR RESULTING FROM THE USE, MODIFICATION, OR
 * DISTRIBUTION OF PROGRAMS OR FILES CREATED FROM, BASED ON, AND/OR
 * DERIVED FROM THIS SOURCE CODE FILE.
 */

class Cache
{
    private final CacheStream[] streams;
    private final int defaultBufferSize;
    private final int elementSize;
    private final CacheReader reader;
    private final CacheWriter writer;

    Cache(int elementSize,
          int bufferSize,
          int fileCount,
          CacheReader reader,
          CacheWriter writer) throws IOException
    {
        this.elementSize = elementSize;
        this.streams = new CacheStream[fileCount];
        this.defaultBufferSize = (bufferSize / this.elementSize) * elementSize;
        this.reader = reader;
        this.writer = writer;
    }

    final ByteBuffer read(int fileIndex,
                          long position) throws IOException
    {
        CacheStream cs;
        if ((cs = streams[fileIndex]) == null)
        {
            cs = new CacheStream(defaultBufferSize, fileIndex, elementSize, reader.readFileLength(fileIndex), reader, writer);
            streams[fileIndex] = cs;
        }
        return cs.read(fileIndex, position);
    }

    final void write(int fileIndex,
                     long position,
                     byte[] data) throws IOException
    {
        CacheStream cs;
        if ((cs = streams[fileIndex]) == null)
        {
            cs = new CacheStream(defaultBufferSize, fileIndex, elementSize, reader.readFileLength(fileIndex), reader, writer);
            streams[fileIndex] = cs;
        }
        cs.write(fileIndex, position, data);
    }

    final long append(int fileIndex,
                      byte[] data) throws IOException
    {
        CacheStream cs;
        if ((cs = streams[fileIndex]) == null)
        {
            cs = new CacheStream(defaultBufferSize, fileIndex, elementSize, reader.readFileLength(fileIndex), reader, writer);
            streams[fileIndex] = cs;
        }
        return cs.append(data);
    }

    final void commit() throws IOException
    {
        for (int i = 0; i < streams.length; i++)
        {
            CacheStream cs = streams[i];
            if (cs != null)
            {
                cs.commit(i);
            }
            streams[i] = null;
        }
    }

    final long fileLength(int fileIndex) throws IOException
    {
        CacheStream cs;
        if ((cs = streams[fileIndex]) == null)
        {
            cs = new CacheStream(defaultBufferSize, fileIndex, elementSize, reader.readFileLength(fileIndex), reader, writer);
            streams[fileIndex] = cs;
        }
        return cs.length();
    }

    final int bufferCount(int fileIndex) throws IOException
    {
        CacheStream cs;
        if ((cs = streams[fileIndex]) == null)
        {
            return 0;
        }
        return cs.bufferCount();
    }

    final int elementCount(int fileIndex, int bufferIndex) throws IOException
    {
        CacheStream cs;
        if ((cs = streams[fileIndex]) == null)
        {
            return 0;
        }
        return cs.elementsCount(bufferIndex);
    }

    final long getMemory(int fileIndex)
    {
        CacheStream cs;
        if ((cs = streams[fileIndex]) == null)
        {
            return 0;
        }
        return cs.getMemory();
    }
}
