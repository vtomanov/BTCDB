package com.tomanov;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

public class MapAByteLong implements CacheReader, CacheWriter
{
    private static final Logger logger = Logger.getLogger(MapAByteLong.class.getCanonicalName());

    private final static int lengthALong = 8;
    private final static String sDAT = ".dat";
    private final static String sDTT = ".dtt";
    private final static String sPREF = "/A";

    private final int fileCount; // 1
    private final int cacheElementCount; //8192 * 1024 / 52 8MB
    private final int diskCacheSize; //8192 * 1024 8MB
    private final RandomAccessFile[] files;
    private final byte[][] cache;
    private final int lengthAByte;
    private final String storageFolderPath;
    private final int elementSize;
    private final int firstKeyLength;
    private final boolean isCompositeKey;

    private int bufferPreferableSize = 1024 * 1024 * 4;// 4M buffer;
    private Cache diskCache = null;

    private static final boolean useBalanceFullCache = true;

    static int cmp(byte[] left,
                   byte[] right,
                   int len)
    {
        int ret;
        for (int i = 0; i < len; ++i)
        {
            byte l = left[i];
            byte r = right[i];
            if ((ret = ((l == r) ? 0 : ((l < r) ? -1 : +1))) != 0)
            {
                return ret;
            }
        }
        return 0;
    }

    public MapAByteLong(int lengthAByte,
                        String storageFolderPath,
                        int firstKeyLength,
                        int diskCacheSize,
                        int hardCacheSize) throws Throwable
    {
        this(lengthAByte, storageFolderPath, firstKeyLength, (hardCacheSize) / (lengthAByte + lengthALong + lengthALong + lengthALong + lengthALong), diskCacheSize, 1); //disckcache =512
    }

    public MapAByteLong(int lengthAByte,
                        String storageFolderPath,
                        int firstKeyLength,
                        int cacheElementCount,
                        int diskCacheSize,
                        int fileCount) throws Throwable
    {
        this.isCompositeKey = firstKeyLength > 0;
        this.lengthAByte = lengthAByte;
        this.storageFolderPath = storageFolderPath;
        this.elementSize = lengthAByte + lengthALong + lengthALong + lengthALong + lengthALong; // key + value + left + right + father
        this.firstKeyLength = firstKeyLength;
        this.fileCount = fileCount;
        this.cacheElementCount = cacheElementCount;
        this.diskCacheSize = diskCacheSize;

        this.files = new RandomAccessFile[fileCount];
        this.cache = new byte[fileCount][];

        if (!new File(storageFolderPath).exists())
        {
            new File(storageFolderPath).mkdirs();
        }
        try
        {
            openFiles();
            fixFiles();
        }
        catch (Throwable th)
        {
            closeFiles();
            throw new Throwable("Files need recreation.");
        }
    }

    final ByteBuffer read(int fileIndex,
                          long position) throws IOException
    {
        //reads++;

        if (diskCache != null)
        {
            return diskCache.read(fileIndex, position);
        }

        return readForCache(fileIndex, position, elementSize);
    }

    final void write(int fileIndex,
                     long position,
                     byte[] data) throws IOException
    {
        //writes++;

        if (diskCache != null)
        {
            diskCache.write(fileIndex, position, data);
            return;
        }

        writeForCache(fileIndex, position, data, data.length);
    }

    private void openFiles() throws IOException
    {
        for (int i = 0; i < fileCount; i++)
        {
            files[i] = new RandomAccessFile(storageFolderPath + sPREF + i + sDAT, "rw");
            int sizeToCache = elementSize * cacheElementCount;
            if (((long) sizeToCache) > files[i].length())
            {
                sizeToCache = (int) ((files[i].length() / (long) elementSize) * (long) elementSize);
            }
            cache[i] = new byte[sizeToCache];
            files[i].seek(0L);
            files[i].readFully(cache[i]);
        }
    }

    final void fixFiles() throws Exception
    {
        for (int i = 0; i < fileCount; i++)
        {
            if ((files[i].length() % (long) elementSize) != 0L)
            {
                files[i].setLength(((files[i].length() / (long) elementSize)) * (long) elementSize);
                logger.info("File : " + storageFolderPath + sPREF + i + sDAT + " size adjusted.");
            }
        }
    }

    private void closeFiles()
    {
        for (RandomAccessFile f : files)
        {
            if (f != null)
            {
                try
                {
                    f.close();
                }
                catch (Throwable the)
                {
                    // not interested
                }
            }
        }
    }

    /**
     * Call to recreate the caches of the files
     *
     * @throws Exception
     */
    public final void flush() throws Exception
    {
        closeFiles();
        try
        {
            openFiles();
        }
        catch (Throwable th)
        {
            closeFiles();
            throw new Exception(th.getMessage());
        }
    }

    /**
     * Call at shutdown ( e/g/ in ShutdownHook)
     */
    public final void close()
    {
        closeFiles();
    }

    public final int getFileCount()
    {
        return fileCount;
    }

    public boolean isTransactionStarted()
    {
        return diskCache != null;
    }

    public final boolean isValidSize(ByteBuffer buffer, AtomicBoolean reachedUnrecoverablePoint) throws Exception
    {

        for (int i = 0; i < fileCount; i++)
        {
            long wantedLength;
            try
            {
                wantedLength = buffer.getLong();
            }
            catch (Exception ex)
            {
                return false;
            }
            if (wantedLength < 0)
            {
                reachedUnrecoverablePoint.set(true);
                return false;
            }
            if (fileLength(i) < wantedLength)
            {
                return false;
            }
        }
        return true;

    }

    /**
     * Return value by key
     *
     * @param key - byte[] with size defined in the constructor
     * @return value - of if key do not exists - null
     */
    public final Long get(byte[] key)
    {
        try
        {
            int fileIndex = getFileIndex(key);
            if (fileLength(fileIndex) == 0L)
            {
                return null;
            }
            long pos = 0L;
            for (ByteBuffer current = read(fileIndex, pos); current != null; current = read(fileIndex, pos))
            {
                byte[] key_n = new byte[lengthAByte];
                current.get(key_n);

                long value = current.getLong();
                long left = current.getLong();

                int cmp = cmp(key, key_n, lengthAByte);
                if (cmp == 0)
                {
                    return new Long(value);
                }
                if (cmp < 0)
                {
                    if ((pos = left) > 0L)
                    {
                        continue;
                    }
                    break;
                }

                if ((pos = current.getLong()) > 0L)// right
                {
                    continue;
                }

                break;
            }

            return null;

        }
        catch (Throwable th)
        {
            return null;
        }
    }

    final long append(int fileIndex,
                      byte[] key,
                      long newValue,
                      long parent) throws IOException
    {
        try
        {
            byte[] b = new byte[elementSize];
            ByteBuffer.wrap(b).put(key).putLong(newValue).putLong(0L).putLong(0L).putLong(parent);

            if (diskCache != null)
            {
                long appendPosition = diskCache.append(fileIndex, b);
                return appendPosition;
            }

            long ret = fileLength(fileIndex);
            files[fileIndex].seek(ret);
            files[fileIndex].write(b);

            return ret;
        }
        catch (Throwable th)
        {
            return -1;
        }
    }

    /**
     * Insert/replace a key/value pair
     *
     * @param key      - byte[] with size defined in the constructor
     * @param newValue - the value to be inserted or replaced
     * @return if the key do not exists - null, else previous value
     */

    public final Long put(byte[] key, long newValue)
    {
        try
        {

            int fileIndex = getFileIndex(key);

            if (fileLength(fileIndex) == 0L)
            {
                // first element
                append(fileIndex, key, newValue, 0L);
                return null;
            }
            long pos = 0L;
            for (ByteBuffer current = read(fileIndex, pos); current != null; current = read(fileIndex, pos))
            {
                byte[] key_n = new byte[lengthAByte];
                current.get(key_n);

                long value = current.getLong();
                long left = current.getLong();

                int cmp = cmp(key, key_n, lengthAByte);
                if (cmp == 0)
                {
                    if (value != newValue)
                    {
                        byte[] b = new byte[lengthALong];
                        ByteBuffer.wrap(b).putLong(newValue);
                        write(fileIndex, pos + ((long) lengthAByte), b);
                    }
                    return value;
                }
                if (cmp < 0)
                {
                    if (left > 0L)
                    {
                        pos = left;
                        continue;
                    }

                    long appPos = append(fileIndex, key, newValue, pos);
                    byte[] b = new byte[lengthALong];
                    ByteBuffer.wrap(b).putLong(appPos);
                    write(fileIndex, pos + (long) lengthAByte + (long) lengthALong, b); // left
                    return null;
                }

                long right = current.getLong();// right
                if (right > 0L)
                {
                    pos = right;
                    continue;
                }

                long appPos = append(fileIndex, key, newValue, pos);
                byte[] b = new byte[lengthALong];
                ByteBuffer.wrap(b).putLong(appPos);
                write(fileIndex, pos + (long) lengthAByte + (long) lengthALong + (long) lengthALong, b); // right
                return null;
            }

            throw new Exception("Internal error");
        }
        catch (Throwable th)
        {
            return null;
        }
    }

    private int getFileIndex(byte[] key)
    {
        int fileIndex;
        if (!isCompositeKey)
        {
            fileIndex = ((0xFF) & key[lengthAByte >> 1]) % fileCount;
        }
        else
        {
            byte middleByteTran = key[firstKeyLength >> 1];
            byte hashMiddleByte = key[firstKeyLength + ((lengthAByte - firstKeyLength) >> 1)];
            fileIndex = (((0xFF & hashMiddleByte) * 256) + (0xFF & middleByteTran)) % fileCount;
        }
        return fileIndex;
    }

    /**
     * Remove a key/value pair
     * The implementation supports remove ONLY of the last element inserted
     * The implementation does not support beginTransaction and endTransaction
     *
     * @param key - byte[] with size defined in the constructor
     * @return the removed value - if null - nothing has been removed
     */
    public final Long remove(byte[] key)
    {
        try
        {
            int fileIndex = getFileIndex(key);
            long pos = 0;
            for (ByteBuffer current = read(fileIndex, pos); current != null; current = read(fileIndex, pos))
            {
                byte[] key_n = new byte[lengthAByte];
                current.get(key_n);

                long value = current.getLong();
                long left = current.getLong();

                int cmp = cmp(key, key_n, lengthAByte);
                if (cmp == 0)
                {
                    if (pos != fileLength(fileIndex) - ((long) elementSize))
                    {
                        throw new RuntimeException("Internal error: try to remove non-last element. This is not permitted.");
                    }

                    long right = current.getLong(); // we just need to read it to be able to read the parent
                    long parent = current.getLong();

                    files[fileIndex].setLength(pos);

                    if (pos < (long) cache[fileIndex].length)
                    {
                        byte[] newCache = new byte[(int) pos];
                        System.arraycopy(cache[fileIndex], 0, newCache, 0, (int) pos);
                        cache[fileIndex] = newCache;
                    }
                    // check that we are not removing the first element  - e.g. first element do not need handling
                    if (pos != 0L)
                    {
                        ByteBuffer parentBuffer = read(fileIndex, parent);
                        byte[] parentKey = new byte[lengthAByte];
                        parentBuffer.get(parentKey);
                        long parentValue = parentBuffer.getLong(); // just read it to be able to read the left
                        long parentLeft = parentBuffer.getLong();
                        if (parentLeft == pos)
                        {
                            // we are the left
                            // just clean the parent left
                            byte[] b = new byte[lengthALong];
                            ByteBuffer.wrap(b).putLong(0L);
                            write(fileIndex, parent + (long) lengthAByte + (long) lengthALong, b); // left
                        }
                        else
                        {
                            // we are the right
                            // just clean the parent right
                            byte[] b = new byte[lengthALong];
                            ByteBuffer.wrap(b).putLong(0L);
                            write(fileIndex, parent + (long) lengthAByte + (long) lengthALong + (long) lengthALong, b); // right
                        }
                    }
                    return value;
                }
                if (cmp < 0)
                {
                    if ((pos = left) > 0L)
                    {
                        continue;
                    }
                    break;
                }

                if ((pos = current.getLong()) > 0L)// right
                {
                    continue;
                }

                break;
            }

            return null;

        }
        catch (Throwable th)
        {
            return null;
        }
    }

    final void traverse(int fileIndex,
                        long pos,
                        TempBalanceProvider buffOut,
                        AtomicLong writeCounter) throws Exception
    {

        ByteBuffer current = read(fileIndex, pos);

        if (current == null)
        {
            return;
        }
        byte[] keyVal = new byte[lengthAByte + lengthALong];
        current.get(keyVal);
        long left = current.getLong();
        long right = current.getLong();

        if (left != 0L)
        {
            traverse(fileIndex, left, buffOut, writeCounter);
        }

        buffOut.write(keyVal);

        if (right != 0L)
        {
            traverse(fileIndex, right, buffOut, writeCounter);
        }
    }


    private void traverseBreadth(TempBalanceProvider tempBalanceProvider, long start, long end,
                                 AtomicLong writeCounter, AtomicBoolean cacheUpdated) throws Exception
    {
        Queue<Node> queue = new LinkedList<Node>();
        Node current = new Node(start, end);
        queue.add(current);
        while ((current = queue.poll()) != null)
        {
            long st = current.getStart();
            long en = current.getEnd();
            if (st <= en)
            {
                long mid = (st + en) / 2;
                tempBalanceProvider.seek(mid * ((long) (lengthAByte + lengthALong)));
                byte[] key = new byte[lengthAByte];
                tempBalanceProvider.read(key);
                long value = tempBalanceProvider.readLong();

                put(key, value);

                queue.add(new Node(mid + 1, en));
                queue.add(new Node(st, mid - 1));

                if (!cacheUpdated.get() && writeCounter.get() >= 2 * cacheElementCount)
                {
                    updateCache();
                    cacheUpdated.set(true);
                }
            }
        }
    }


    private int traverseDepth(int fileIndex,
                              long pos,
                              int depth) throws IOException
    {
        ByteBuffer current = read(fileIndex, pos);
        if (current == null)
        {
            return depth;
        }
        byte[] keyVal = new byte[lengthAByte + lengthALong];
        current.get(keyVal);
        long left = current.getLong();
        long right = current.getLong();
        int retL = depth;
        int retR = depth;
        if (left != 0L)
        {
            retL = traverseDepth(fileIndex, left, depth + 1);
        }

        if (right != 0L)
        {
            retR = traverseDepth(fileIndex, right, depth + 1);
        }

        return Math.max(retL, retR);
    }

    /**
     * Max depth of the binary tree.
     * Use with CARE - this method traverse the whole structure and is designed to be used only for logging or VERY unfrequented use
     * Zero based counting
     *
     * @return current max depth
     */
    public final int getMaxDepth()
    {
        int depth = 0;
        try
        {
            for (int i = 0; i < fileCount; i++)
            {
                depth = Math.max(depth, traverseDepth(i, 0, 0));
            }
        }
        catch (Throwable th)
        {
            //
        }
        return depth;
    }

    public final void getCurrentFilePositions(ByteBuffer store)
    {
        for (int i = 0; i < fileCount; i++)
        {
            try
            {
                store.putLong(fileLength(i));//Where to start over
            }
            catch (Throwable ex)
            {
                //
            }
        }
    }

    public final void setInvalidPosition(ByteBuffer store)
    {
        for (int i = 0; i < fileCount; i++)
        {
            try
            {
                store.putLong(-1);//Where to start over
            }
            catch (Throwable ex)
            {
                //
            }
        }
    }

    public final boolean setFileLength(ByteBuffer buffer)
    {
        logger.info("Truncating files for recovery ... " + "[" + storageFolderPath + "]");
        for (int i = 0; i < fileCount; i++)
        {
            try
            {
                long newLength = buffer.getLong();
                if (files[i].length() == newLength)
                {
                    //no need to recover
                    return false;
                }
                files[i].setLength(newLength);
            }
            catch (Throwable ex)
            {
                //
            }
        }
        try
        {
            recoverFix();
        }
        catch (Exception ex)
        {
            //
        }
        return true;
    }

    /**
     * Force a binary tree balance of the current structure.
     * <p/>
     * Use with CARE - this method traverse the whole structure multiple time and is designed to be used only for VERY unfrequented use
     */
    public final void balance()
    {
        TempBalanceProvider[] balanceProviders = new TempBalanceProvider[fileCount];
        try
        {
            // build sorted files
            for (int fileIndex = 0; fileIndex < fileCount; fileIndex++)
            {
                String fileBaseName = storageFolderPath + sPREF + fileIndex;
                if (new File(fileBaseName + sDTT).exists())
                {
                    if (!new File(fileBaseName + sDTT).delete())
                    {
                        logger.severe("Cannot clean file : " + fileBaseName + sDTT);
                        throw new IOException("Cannot clean files");
                    }
                }

                logger.info("Staring sorted file creation for : " + fileBaseName + sDTT);


                try
                {
                    if (useBalanceFullCache)
                    {
                        balanceProviders[fileIndex] = new TempBalanceProviderMemoryImpl(files[fileIndex].length(), elementSize, lengthAByte + lengthALong);
                    }
                    else
                    {
                        balanceProviders[fileIndex] = new TempBalanceProviderFileImpl(new File(fileBaseName + sDTT));
                    }


                    traverse(fileIndex, 0, balanceProviders[fileIndex], new AtomicLong(0));
                }
                catch (Throwable the)
                {
                    //
                }
                finally
                {

                    balanceProviders[fileIndex].closeWriter();
                }

                logger.info("Sorted file creation done for : " + fileBaseName + sDTT);
            }

            // now clean the originals

            logger.info("Start cleaning files.");
            boolean needBeginTransaction = false;
            if (diskCache != null)
            {
                endTransaction();
                needBeginTransaction = true;
            }

            closeFiles();

            for (int fileIndex = 0; fileIndex < fileCount; fileIndex++)
            {
                String fileBaseName = storageFolderPath + sPREF + fileIndex;
                if (new File(fileBaseName + sDAT).exists())
                {
                    if (!new File(fileBaseName + sDAT).delete())
                    {
                        logger.severe("Cannot clean file : " + fileBaseName + sDAT);
                        throw new IOException("Cannot clean files");
                    }
                }
            }

            openFiles();

            if (needBeginTransaction)
            {
                beginTransaction();
            }

            logger.info("File cleaning done.");
            // now fill
            for (int i = 0; i < fileCount; i++)
            {
                logger.info("Start recreating : " + storageFolderPath + sPREF + i + sDTT);
                RandomAccessFile raf = null;
                try
                {
                    long length = balanceProviders[i].length();
                    if (length > 0L)
                    {
                        long elementCount = (length / ((long) lengthAByte + (long) lengthALong));
                        traverseBreadth(balanceProviders[i], 0, elementCount - 1, new AtomicLong(0), new AtomicBoolean(false));
                    }
                }
                catch (Throwable the)
                {
                    //
                }
                finally
                {
                    if (raf != null)
                    {
                        try
                        {
                            raf.close();
                        }
                        catch (Throwable thex)
                        {
                            // not interested
                        }

                        try
                        {
                            if (!new File(storageFolderPath + sPREF + i + sDTT).delete())
                            {
                                logger.severe("Cannot clean file : " + storageFolderPath + sPREF + i + sDTT);
                                throw new IOException("Cannot clean files");
                            }
                        }
                        catch (Throwable thex)
                        {
                            // not interested
                        }
                    }
                }
                logger.info("Recreating done for : " + storageFolderPath + sPREF + i + sDTT);

            }
        }
        catch (Throwable th)
        {
            //
        }
    }

    private void recoverFix() throws Exception
    {
        logger.info("Start index cleanup ..." + "[" + storageFolderPath + "]");
        bufferPreferableSize = (bufferPreferableSize / elementSize) * elementSize;//buffer should have size multiple of elementSize
        byte[] array = new byte[bufferPreferableSize];

        for (int fileIndex = 0; fileIndex < fileCount; fileIndex++)
        {
            long pos = 0L;
            files[fileIndex].seek(pos);
            for (int bytesRead; (bytesRead = files[fileIndex].read(array)) > 0; )
            {
                ByteBuffer buffer = ByteBuffer.wrap(array, 0, bytesRead);
                boolean hasChanges = false;
                for (; buffer.hasRemaining(); )
                {
                    buffer.position(buffer.position() + lengthAByte + lengthALong); //+ value
                    // left
                    if (buffer.getLong() >= fileLength(fileIndex))
                    {
                        // clear left
                        buffer.position(buffer.position() - lengthALong);
                        buffer.putLong(0L);
                        hasChanges = true;
                    }
                    // right
                    if (buffer.getLong() >= fileLength(fileIndex))
                    {
                        // clear right
                        buffer.position(buffer.position() - lengthALong);
                        buffer.putLong(0L);
                        hasChanges = true;
                    }
                    // skip father
                    buffer.position(buffer.position() + lengthALong);
                }
                // check if we need to write in case of changes
                if (hasChanges)
                {
                    // we have adjustments in this buffer please write it
                    files[fileIndex].seek(pos);
                    files[fileIndex].write(array, 0, bytesRead);
                }

                // seek for next read
                pos += bytesRead;
                files[fileIndex].seek(pos);
            }
        }
        logger.info("Start cache update ..." + "[" + storageFolderPath + "]");
        updateCache();
        logger.info("Recovery done " + "[" + storageFolderPath + "]");
    }

    private final void updateCache() throws Exception
    {
        logger.info("UPDATE CACHE. WILL REOPEN");

        boolean shouldBeginTransaction = false;
        if (isTransactionStarted())
        {
            shouldBeginTransaction = true;
            endTransaction();
        }
        // close and reopen to update cache
        closeFiles();
        openFiles();

        if (shouldBeginTransaction)
        {
            beginTransaction();
        }
    }

    /**
     * Retrieves the file size
     *
     * @param fileIndex - the index of the file which size we want to retrieve
     * @return the size of the file with certain index
     */
    public final long fileLength(int fileIndex)
    {
        try
        {
            if (diskCache != null)
            {
                return diskCache.fileLength(fileIndex);
            }

            return readFileLength(fileIndex);
        }
        catch (IOException ex)
        {
            //
            return 0;
        }
    }

    /**
     * Call when the disk cache should be used
     * Does not work with the remove operation
     *
     * @throws Exception
     */
    public final void beginTransaction() throws Exception
    {
        if (diskCache != null)
        {
            return;
        }

        diskCache = new Cache(elementSize, diskCacheSize, fileCount, this, this);
    }

    /**
     * Call when the operations with the disk cache have finished
     * Does not work with remove operation
     * beginTransaction should be called first
     *
     * @throws Exception
     */
    public final void endTransaction() throws Exception
    {
        if (diskCache == null)
        {
            return;
        }

        diskCache.commit();
        diskCache = null;
    }

    public final boolean containsKey(byte[] key)
    {
        try
        {
            int fileIndex = getFileIndex(key);
            if (fileLength(fileIndex) == 0L)
            {
                return false;
            }
            long pos = 0L;
            for (ByteBuffer current = read(fileIndex, pos); current != null; current = read(fileIndex, pos))
            {
                byte[] key_n = new byte[lengthAByte];
                current.get(key_n);

                long value = current.getLong();  //Just read it;
                long left = current.getLong();

                int cmp = cmp(key, key_n, lengthAByte);
                if (cmp == 0)
                {
                    return true;
                }
                if (cmp < 0)
                {
                    if (left > 0L)
                    {
                        pos = left;
                        continue;
                    }
                    break;
                }

                long right = current.getLong();// right
                if (right > 0L)
                {
                    pos = right;
                    continue;
                }
                break;
            }
            return false;
        }
        catch (Throwable th)
        {
            //
            return false;
        }
    }

    @Override
    public final ByteBuffer readForCache(int fileIndex, long position, int size) throws IOException
    {
        if (cache[fileIndex].length >= (position + size))
        {
            byte[] ret = new byte[size];
            System.arraycopy(cache[fileIndex], (int) position, ret, 0, size);
            return ByteBuffer.wrap(ret);
        }

        byte[] ret = new byte[size];
        ByteBuffer retBuf = ByteBuffer.wrap(ret);

        files[fileIndex].seek(position);
        int elemCount = files[fileIndex].read(ret);
        retBuf.limit((elemCount >= 0) ? elemCount : 0);
        return retBuf;
    }

    @Override
    public final long readFileLength(int fileIndex) throws IOException
    {
        return files[fileIndex].length();
    }

    @Override
    public final void writeForCache(int fileIndex, long position, byte[] data, int length) throws IOException
    {
        if (cache[fileIndex].length >= (position + length))
        {
            System.arraycopy(data, 0, cache[fileIndex], (int) position, length);
        }
        else if (cache[fileIndex].length > position)
        {
            System.arraycopy(data, 0, cache[fileIndex], (int) position, (int) (cache[fileIndex].length - position));
        }

        files[fileIndex].seek(position);
        files[fileIndex].write(data, 0, length);

    }

}
