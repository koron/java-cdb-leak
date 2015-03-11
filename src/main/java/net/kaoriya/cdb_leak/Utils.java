package net.kaoriya.cdb_leak;

import java.io.File;
import java.util.Random;

public class Utils {

    public static boolean makeParents(File f) {
        File dir = f.getParentFile();
        if (dir != null && !dir.exists()) {
            return dir.mkdirs();
        }
        return true;
    }

    public static byte[] randomBytes(Random r, int len) {
        byte[] b = new byte[len];
        r.nextBytes(b);
        return b;
    }

}
