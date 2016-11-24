package com.tomanov;

import java.io.*;
import java.util.logging.Logger;

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


class TempBalanceProviderFileImpl implements TempBalanceProvider
{
    private static final Logger logger = Logger.getLogger(TempBalanceProviderFileImpl.class.getCanonicalName());

    private final File file;
    private BufferedOutputStream buffOut = null;
    private RandomAccessFile raf = null;

    TempBalanceProviderFileImpl(File file) throws Exception
    {
        this.file = file;
        buffOut = new BufferedOutputStream(new FileOutputStream(file));
    }

    @Override
    public void write(byte[] bytes) throws Exception
    {
        buffOut.write(bytes);
    }

    @Override
    public void read(byte[] bytes) throws Exception
    {
        raf.read(bytes);
    }

    @Override
    public long readLong() throws Exception
    {
        return raf.readLong();
    }

    @Override
    public void seek(long position) throws Exception
    {
        raf.seek(position);
    }

    @Override
    public void closeWriter() throws Exception
    {
        if (buffOut != null)
        {
            buffOut.close();
        }
        raf = new RandomAccessFile(file, "r");
    }

    @Override
    public void closeReader() throws Exception
    {
        if (raf != null)
        {
            raf.close();
        }
        if (!file.delete())
        {
            logger.severe("Cannot clean file : " + file.getCanonicalPath());
            throw new IOException("Cannot clean files");
        }
    }

    @Override
    public long length() throws Exception
    {
        return raf.length();
    }
}
