package net.kaoriya.cdb_leak;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Map;

import ca.hullabaloo.cdb.Cdb;

public class Main {

    public static File DIR = new File("var/cdb");
    public static int FILE_COUNT = 100;
    public static long END_WAIT_SEC = 30;

    public static int KEY_LEN = 64;
    public static int VALUE_LEN = 192;
    public static int ENTRY_COUNT = 100000;

    public static float HIT_RATE = 0.75f;
    public static long KEY_SEED = 0l;

    public static float QUERY_RATE = 1.5f;

    public static byte[][] keys;

    public static void main(String[] args) throws Exception {
        generateKeys(KEY_SEED);
        buildAllFiles();
        leakTest();
        endWait();
        //removeAllFiles();
    }

    static void generateKeys(long seed) throws Exception {
        Random r = new Random(seed);
        int c = (int)(((float)ENTRY_COUNT / HIT_RATE) + 0.5f);
        keys = new byte[c][];
        for (int i = 0; i < c; ++i) {
            keys[i] = Utils.randomBytes(r, KEY_LEN);
        }
    }

    static void buildAllFiles() throws Exception {
        System.out.println("building data files");
        Random r = new Random();
        int count = 0;
        for (int i = 0; i < FILE_COUNT; ++i) {
            File f = getFile(i);
            if (f.exists()) {
                continue;
            }
            if (!Utils.makeParents(f)) {
                throw new Exception("failed to create dir for " + f.toString());
            }
            buildFile(f, r);
            ++count;
            if ((count % (FILE_COUNT / 10)) == 0) {
                System.out.printf("  %d/%d data files generated",
                        count, FILE_COUNT);
                System.out.println();
            }
        }
        if (count == 0) {
            System.out.println("  building data files, skipped");
        }
    }

    static void removeAllFiles() throws Exception {
        System.out.println("cleanup data files");
        for (int i = 0; i < FILE_COUNT; ++i) {
            getFile(i).delete();
        }
    }

    static File getFile(int n) throws Exception {
        return new File(DIR, "data-" + Integer.toString(n) + ".cdb");
    }

    static byte[][] chooseKeys(Random r, int count) {
        // shuffle keys reference table.
        int[] keyRef = new int[keys.length];
        for (int i = 0; i < keyRef.length; ++i) {
            keyRef[i] = i;
        }
        for (int i = keyRef.length - 1; i > 1; --i) {
            int j = r.nextInt(i);
            int t = keyRef[i];
            keyRef[i] = keyRef[j];
            keyRef[j] = t;
        }
        // Project shuffled keyRef to key array.
        byte[][] retval = new byte[count][];
        for (int i = 0; i < count; ++i) {
            retval[i] = keys[keyRef[i]];
        }
        return retval;
    }

    static void buildFile(File f, Random r) throws Exception {
        Cdb.Builder b = Cdb.builder(f);
        try {
            byte[][] tmpKeys = chooseKeys(r, ENTRY_COUNT);
            for (int i = 0; i < ENTRY_COUNT; ++i) {
                byte[] key = tmpKeys[i];
                byte[] value = Utils.randomBytes(r, VALUE_LEN);
                b.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
            }
        } finally {
            b.close();
        }
    }

    static void leakTest() throws Exception {
        System.out.println("testing leak");
        System.gc();
        Random r = new Random();
        for (int i = 0; i < 10000; ++i) {
            File f = getFile(i % FILE_COUNT);
            queryCdb(f, r);
        }
    }

    static void queryCdb(File f, Random r) throws Exception {
        Map<ByteBuffer, ByteBuffer> m = Cdb.open(f);
        int queryCount = (int)((ENTRY_COUNT * QUERY_RATE) + 0.5f);
        int hitCount = 0;
        for (int i = 0; i < queryCount; ++i) {
            byte[] k = keys[r.nextInt(keys.length)];
            ByteBuffer v = m.get(ByteBuffer.wrap(k));
            if (v != null) {
                ++hitCount;
            }
        }
        System.out.printf("  %s hit-rate:%.3f",
                f.getName(), (double)hitCount / queryCount);
        System.out.println();
    }

    static void endWait() throws Exception {
        System.out.println("waiting " + END_WAIT_SEC + " secs to end");
        Thread.sleep(END_WAIT_SEC * 1000);
    }

}
