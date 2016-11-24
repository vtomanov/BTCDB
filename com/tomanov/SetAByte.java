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

public class SetAByte implements CacheReader, CacheWriter
{
    private static final Logger logger = Logger.getLogger(SetAByte.class.getCanonicalName());
    private final int fileCount;
    private final int cacheElementCount;
    private final int diskCacheSize;
    private final static String sDAT = ".dat";
    private final static String sDTT = ".dtt";
    private final static String sPREF = "/A";

    private final static int lengthALong = 8;
    private int bufferPreferableSize = 1024 * 1024 * 4;// 4M buffer;

    private final RandomAccessFile[] files;
    private final byte[][] cache;
    private Cache diskCache = null;
    private final int lengthAByte;
    private final String storageFolderPath;
    private final int elementSize;
    private final int firstKeyLength;
    private final boolean isCompositeKey;

    private static final boolean useBalanceFullCache = true;

    private static final long printInterval = 10000;

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

    /**
     * Constructs a key index file
     *
     * @param lengthAByte       - key is expected to be byte[] and initialized with len = lengthAByte;
     * @param storageFolderPath - path to a folder where the necessary files will be stored (make sure that the user has right to read/write in the folder )
     */
    public SetAByte(int lengthAByte,
                    String storageFolderPath) throws Throwable
    {
        this(lengthAByte, storageFolderPath, -1, 512, 8192 * 1024);
    }

    public SetAByte(int lengthAByte,
                    String storageFolderPath,
                    int firstKeyLength,
                    int diskCacheSize,
                    int hardCacheSize) throws Throwable
    {
        this(lengthAByte, storageFolderPath, firstKeyLength, (hardCacheSize) / (lengthAByte + lengthALong + lengthALong + lengthALong), diskCacheSize, 1);
    }

    public SetAByte(int lengthAByte,
                    String storageFolderPath,
                    int firstKeyLength,
                    int cacheElementCount,
                    int diskCacheSize,
                    int fileCount) throws Throwable
    {
        this.fileCount = fileCount;
        this.cacheElementCount = cacheElementCount;
        this.diskCacheSize = diskCacheSize;
        this.isCompositeKey = (firstKeyLength > 0);
        this.lengthAByte = lengthAByte;
        this.storageFolderPath = storageFolderPath;
        this.elementSize = lengthAByte + lengthALong + lengthALong + lengthALong; // key  + left + right + father
        this.firstKeyLength = firstKeyLength;

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


    private ByteBuffer read(int fileIndex,
                            long position) throws IOException
    {
        if (diskCache != null)
        {
            return diskCache.read(fileIndex, position);
        }

        return readForCache(fileIndex, position, elementSize);
    }

    private void write(int fileIndex,
                       long position,
                       byte[] data) throws IOException
    {
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
                sizeToCache = (int) files[i].length();
            }
            cache[i] = new byte[sizeToCache];
            files[i].seek(0L);
            files[i].readFully(cache[i]);
        }
    }

    private void fixFiles() throws Exception
    {
        for (int i = 0; i < fileCount; i++)
        {
            if (((long) files[i].length() % (long) elementSize) != 0L)
            {
                files[i].setLength((((long) files[i].length() / (long) elementSize)) * (long) elementSize);
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
     * Call to force flush of all data to disk
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

    /**
     * Returns the current size
     *
     * @return the current size ( count of elements ) in the structure
     */
    public final long size()
    {
        try
        {
            long ret = 0;
            for (int fileIndex = 0; fileIndex < fileCount; fileIndex++)
            {
                ret += (fileLength(fileIndex) / ((long) elementSize));
            }
            return ret;
        }
        catch (Throwable th)
        {
            //
        }

        return 0L;
    }

    /**
     * Return whether key is present
     *
     * @param key - byte[] with size defined in the constructor
     * @return true if the key exists and false otherwise
     */
    public final boolean containsKey(byte[] key)
    {
        try
        {
            int fileIndex = getFileIndex(key);

            if (fileLength(fileIndex) == 0)
            {
                return false;
            }

            long pos = 0L;
            for (ByteBuffer current = read(fileIndex, pos); current != null; current = read(fileIndex, pos))
            {
                byte[] key_n = new byte[lengthAByte];
                current.get(key_n);

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
                long right = current.getLong();
                if (right > 0L)// right
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
            return false;
        }
        finally
        {
            Thread.yield();
        }
    }

    final long append(int fileIndex, byte[] key, long parent)
    {
        try
        {
            byte[] b = new byte[elementSize];
            ByteBuffer.wrap(b).put(key).putLong(0L).putLong(0L).putLong(parent);

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
     * Insert a key
     *
     * @param key - byte[] with size defined in the constructor
     */
    public final void put(byte[] key) throws Exception
    {
        try
        {
            int fileIndex = getFileIndex(key);

            if (fileLength(fileIndex) == 0L)
            {
                // first element
                append(fileIndex, key, 0L);
                return;
            }
            long pos = 0L;
            for (ByteBuffer current = read(fileIndex, pos); current != null; current = read(fileIndex, pos))
            {
                byte[] key_n = new byte[lengthAByte];
                current.get(key_n);

                long left = current.getLong();

                int cmp = cmp(key, key_n, lengthAByte);
                if (cmp == 0)
                {
                    throw new Exception("Try to add an already existing element in a hash set.");
                }
                if (cmp < 0)
                {
                    if (left > 0L)
                    {
                        pos = left;
                        continue;
                    }
                    long appPos = append(fileIndex, key, pos);
                    byte[] b = new byte[lengthALong];
                    ByteBuffer.wrap(b).putLong(appPos);
                    write(fileIndex, pos + (long) lengthAByte, b); // left
                    return;
                }

                long right = current.getLong();// right
                if (right > 0L)
                {
                    pos = right;
                    continue;
                }

                long appPos = append(fileIndex, key, pos);
                byte[] b = new byte[lengthALong];
                ByteBuffer.wrap(b).putLong(appPos);
                write(fileIndex, pos + (long) lengthAByte + (long) lengthALong, b); // right
                return;
            }

            throw new Exception("Internal error");

        }
        catch (Exception ex)
        {
            throw ex;
        }
    }

    /**
     * Remove a key
     * <p/>
     * The implementation supports remove ONLY of the last element inserted
     * The implementation does not support usage of disk cache
     *
     * @param key - byte[] with size defined in the constructor
     * @return the boolean stating whether the key was removed
     */
    public final boolean remove(byte[] key)
    {
        try
        {
            int fileIndex = getFileIndex(key);
            long pos = 0;
            for (ByteBuffer current = read(fileIndex, pos); current != null; current = read(fileIndex, pos))
            {
                byte[] key_n = new byte[lengthAByte];
                current.get(key_n);

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
                        long parentLeft = parentBuffer.getLong();
                        if (parentLeft == pos)
                        {
                            // just clean the parent left
                            byte[] b = new byte[lengthALong];
                            ByteBuffer.wrap(b).putLong(0L);
                            write(fileIndex, parent + (long) lengthAByte, b); // left
                        }
                        else
                        {
                            // we are the right
                            // just clean the parent right
                            byte[] b = new byte[lengthALong];
                            ByteBuffer.wrap(b).putLong(0L);
                            write(fileIndex, parent + (long) lengthAByte + (long) lengthALong, b); // right
                        }
                    }
                    return true;
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

            return false;

        }
        catch (Throwable th)
        {
            return false;
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
        byte[] key = new byte[lengthAByte];
        current.get(key);
        long left = current.getLong();
        long right = current.getLong();

        if (left != 0L)
        {
            traverse(fileIndex, left, buffOut, writeCounter);
        }

        if (writeCounter.incrementAndGet() % printInterval == 0)
        {
            logger.info("Balance phase 1 in progress. " + printInterval + " more elements written. Total: " + writeCounter.get());
        }

        buffOut.write(key);

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
                tempBalanceProvider.seek(mid * ((long) (lengthAByte)));
                byte[] key = new byte[lengthAByte];
                tempBalanceProvider.read(key);

                if (writeCounter.incrementAndGet() % printInterval == 0)
                {
                    logger.info("Balance phase 2 in progress. " + printInterval + " more elements written. Total: " + writeCounter.get());
                }


                put(key);

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
        byte[] key = new byte[lengthAByte];
        current.get(key);
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
     * <p/>
     * Use with CARE - this method traverse the whole structure and is designed to be used only for logging or VERY unfrequented use
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

    public void getCurrentFilePositions(ByteBuffer store)
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

    public final boolean setFileLength(ByteBuffer buffer) throws Exception
    {
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
                        balanceProviders[fileIndex] = new TempBalanceProviderMemoryImpl(files[fileIndex].length(), elementSize, lengthAByte);
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

                try
                {
                    long length = balanceProviders[i].length();
                    if (length > 0L)
                    {
                        int elementCount = (int) (length / ((long) lengthAByte));
                        traverseBreadth(balanceProviders[i], 0, elementCount - 1, new AtomicLong(0), new AtomicBoolean(false));
                    }
                }
                catch (Throwable the)
                {
                    //
                }
                finally
                {
                    balanceProviders[i].closeReader();
                }
                logger.info("Recreating done for : " + storageFolderPath + sPREF + i + sDTT);

            }
        }
        catch (Throwable th)
        {
            //

        }
    }

    private final int getFileIndex(byte[] key)
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

    final void recoverFix() throws Exception
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
                    buffer.position(buffer.position() + lengthAByte);

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

    private final boolean containsAny(LinkedList<byte[]> keys, long pos, int fileIndex, AtomicBoolean shouldContinueTraversal)
    {
        try
        {
            if (fileLength(fileIndex) == 0)
            {
                return false;
            }

            LinkedList<byte[]> lefts = new LinkedList<byte[]>();
            LinkedList<byte[]> rights = new LinkedList<byte[]>();
            long posLeft = 0;
            long posRight = 0;

            ByteBuffer current = read(fileIndex, pos);
            if (current != null && shouldContinueTraversal.get())
            {
                byte[] key_n = new byte[lengthAByte];
                current.get(key_n);
                long left = current.getLong();
                long right = current.getLong();
                posLeft = left;
                posRight = right;

                for (byte[] key : keys)
                {
                    int cmp = cmp(key, key_n, lengthAByte);
                    if (cmp == 0)
                    {
                        shouldContinueTraversal.set(false);
                        break;
                    }
                    if (cmp < 0)
                    {
                        if (left > 0L)
                        {
                            lefts.add(key);
                        }
                        continue;
                    }
                    if (right > 0L)// right
                    {
                        rights.add(key);
                    }
                    continue;
                }
            }

            if (lefts.size() > 0)
            {
                containsAny(lefts, posLeft, fileIndex, shouldContinueTraversal);
            }
            if (rights.size() > 0)
            {
                containsAny(rights, posRight, fileIndex, shouldContinueTraversal);
            }
            return !shouldContinueTraversal.get();

        }
        catch (Throwable th)
        {
            //
            return false;
        }
    }

    public final ByteBuffer readForCache(
            int fileIndex,
            long position,
            int size) throws IOException
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
    public final void writeForCache(int fileIndex,
                                    long position,
                                    byte[] data,
                                    int length) throws IOException
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

    public boolean isTransactionStarted()
    {
        return diskCache != null;
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

    public final long readFileLength(int fileIndex) throws IOException
    {
        return files[fileIndex].length();
    }

    private void updateCache() throws Exception
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

    public static void main(String args[])
    {
        try
        {
            String file = args[0];

            SetAByte spentOutputsByTransactions = new SetAByte(52, file, 32, 200000000, 1000000);
            spentOutputsByTransactions.beginTransaction();
            spentOutputsByTransactions.balance();
            spentOutputsByTransactions.endTransaction();
            spentOutputsByTransactions.flush();
        }
        catch (Throwable throwable)
        {
            //
        }
    }
}
