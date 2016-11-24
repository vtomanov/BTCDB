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

class CacheStreamElement
{
    private final long position;
    private final byte[] data;
    private int dataPos;
    private boolean dirty = false;

    CacheStreamElement(long position,
                       byte[] data,
                       int dataPos)
    {
        this.position = position;
        this.data = data;
        this.dataPos = dataPos;
    }

    CacheStreamElement(long position)
    {
        this.position = position;
        this.data = null;
    }

    final void setDirty(boolean dirty)
    {
        this.dirty = dirty;
    }

    final boolean isDirty()
    {
        return dirty;
    }

    final long position()
    {
        return position;
    }

    final int getDataPos()
    {
        return dataPos;
    }

    final void setDataPos(int dataPos)
    {
        this.dataPos = dataPos;
    }

    final ByteBuffer get(int pos, int size)
    {
        return ByteBuffer.wrap(data, pos, size);
    }

    final void set(int pos, byte[] data)
    {
        System.arraycopy(data, 0, this.data, pos, data.length);
    }

    final byte[] data()
    {
        return data;
    }
}
