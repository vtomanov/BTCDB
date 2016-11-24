package com.tomanov;

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

class TempBalanceProviderMemoryImpl implements TempBalanceProvider
{

    private byte[][] memoryStorage = null;
    private final int cacheChunkSize;
    private long pointer = 0;
    private long length = 0;

    TempBalanceProviderMemoryImpl(long size, int elementSize, int dataSize)
    {
        this.cacheChunkSize = dataSize * 1000000;

        long elementsCount = (long) size / (long) elementSize;
        long memoryStorageSize = (long) elementsCount * (long) dataSize;
        long numberOFChunks = (long) memoryStorageSize / (long) cacheChunkSize;
        int numberOFChunksInt = ((int) numberOFChunks) + 1;

        memoryStorage = new byte[numberOFChunksInt][];
    }


    @Override
    public void write(byte[] bytes)
    {
        int chunkToUse = (int) (pointer / cacheChunkSize);
        int positionInChunk = (int) (pointer % cacheChunkSize);
        byte[] chunkData = memoryStorage[chunkToUse];

        if (chunkData == null)
        {
            chunkData = new byte[cacheChunkSize];
            memoryStorage[chunkToUse] = chunkData;
        }

        System.arraycopy(bytes, 0, chunkData, positionInChunk, bytes.length);

        pointer += bytes.length;
        length += bytes.length;
    }

    @Override
    public void read(byte[] bytes)
    {
        int chunkToUse = (int) (pointer / cacheChunkSize);
        int positionInChunk = (int) (pointer % cacheChunkSize);
        byte[] chunkData = memoryStorage[chunkToUse];

        if (chunkData == null)
        {
            chunkData = new byte[cacheChunkSize];
            memoryStorage[chunkToUse] = chunkData;
        }

        System.arraycopy(chunkData, positionInChunk, bytes, 0, bytes.length);
        pointer += bytes.length;
    }

    @Override
    public long readLong() throws Exception
    {
        byte[] longBytes = new byte[8];
        read(longBytes);
        ByteBuffer bb = ByteBuffer.wrap(longBytes);
        return bb.getLong();
    }

    @Override
    public void seek(long position)
    {
        pointer = position;
    }

    @Override
    public void closeWriter() throws Exception
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void closeReader() throws Exception
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long length() throws Exception
    {
        return length;
    }
}
