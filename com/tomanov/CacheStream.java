package com.tomanov;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

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

class CacheStream
{
    private final ArrayList<CacheStreamElement> stream = new ArrayList<CacheStreamElement>();

    private static Comparator<CacheStreamElement> streamElementComparator = new Comparator<CacheStreamElement>()
    {
        @Override
        public int compare(CacheStreamElement cacheStreamElement1, CacheStreamElement cacheStreamElement2)
        {
            long pos1 = cacheStreamElement1.position();
            long pos2 = cacheStreamElement2.position();
            return ((pos1 < pos2) ? -1 : ((pos1 > pos2) ? 1 : 0));
        }
    };

    private final CacheReader reader;
    private final CacheWriter writer;
    private final int defaultBufferSize;
    private final int elementSize;

    CacheStream(int defaultBufferSize,
                int fileIndex,
                int elementSize,
                long fileSize,
                CacheReader reader,
                CacheWriter writer) throws IOException
    {
        this.defaultBufferSize = defaultBufferSize;
        this.elementSize = elementSize;
        this.reader = reader;
        this.writer = writer;
        long readPosition =
                (fileSize % defaultBufferSize) == 0 ?
                        (((fileSize / defaultBufferSize) - 1) * defaultBufferSize) :
                        ((fileSize / defaultBufferSize) * defaultBufferSize);
        if (readPosition >= 0)
        {
            ByteBuffer cacheBuffer = reader.readForCache(fileIndex, readPosition, defaultBufferSize);
            CacheStreamElement cse = new CacheStreamElement(readPosition, cacheBuffer.array(), cacheBuffer.limit());
            stream.add(cse);
        }
    }

    final void write(int fileIndex, long position, byte[] data) throws IOException
    {
        byte[] cacheData;
        long readPosition = (position / defaultBufferSize) * defaultBufferSize;
        int index = Collections.binarySearch(stream, new CacheStreamElement(readPosition), streamElementComparator);
        if (index < 0)
        {
            // need read
            index = (index * (-1)) - 1;
            ByteBuffer cacheBuffer = reader.readForCache(fileIndex, readPosition, defaultBufferSize);
            CacheStreamElement cse = new CacheStreamElement(readPosition, cacheBuffer.array(), cacheBuffer.limit());
            cse.setDirty(true);
            stream.add(index, cse);
            cacheData = cacheBuffer.array();
        }
        else
        {
            CacheStreamElement cse = stream.get(index);
            cse.setDirty(true);
            cacheData = cse.data();
        }

        System.arraycopy(data, 0, cacheData, (int) (position % defaultBufferSize), data.length);
    }

    final ByteBuffer read(int fileIndex, long position) throws IOException
    {
        long readPosition = (position / defaultBufferSize) * defaultBufferSize;
        int index = Collections.binarySearch(stream, new CacheStreamElement(readPosition), streamElementComparator);
        if (index >= 0)
        {
            // we are cached
            return ByteBuffer.wrap(stream.get(index).data(), (int) (position % defaultBufferSize), elementSize);
        }
        // need read
        index = (index * (-1)) - 1;
        ByteBuffer buf = reader.readForCache(fileIndex, readPosition, defaultBufferSize);
        CacheStreamElement element = new CacheStreamElement(readPosition, buf.array(), buf.limit());
        stream.add(index, element);

        return ByteBuffer.wrap(buf.array(), (int) (position % defaultBufferSize), elementSize);
    }

    final void commit(int fileIndex) throws IOException
    {
        for (int i = 0; i < stream.size(); i++)
        {
            CacheStreamElement cse = stream.get(i);
            if (cse.isDirty())
            {
                writer.writeForCache(fileIndex, cse.position(), cse.data(), cse.getDataPos());
            }
        }
    }

    final long append(byte[] data) throws IOException
    {
        CacheStreamElement cse;
        if (stream.size() == 0)
        {
            cse = new CacheStreamElement(0, new byte[defaultBufferSize], 0);
            stream.add(cse);
        }
        else
        {
            cse = stream.get(stream.size() - 1);
            if (cse.getDataPos() >= defaultBufferSize)
            {
                cse = new CacheStreamElement(cse.position() + defaultBufferSize, new byte[defaultBufferSize], 0);
                stream.add(cse);
            }
        }

        cse.setDirty(true);
        long ret = cse.position() + cse.getDataPos();
        System.arraycopy(data, 0, cse.data(), cse.getDataPos(), data.length);
        cse.setDataPos(cse.getDataPos() + data.length);

        return ret;
    }

    final long length()
    {
        if (stream.size() == 0)
        {
            return 0;
        }
        else
        {
            CacheStreamElement cse = stream.get(stream.size() - 1);
            return cse.position() + cse.getDataPos();
        }
    }

    final int bufferCount()
    {
        return stream.size();
    }

    final int elementsCount(int index)
    {
        return stream.get(index).getDataPos() / elementSize;
    }

    final long getMemory()
    {
        return stream.size() * defaultBufferSize;
    }
}
