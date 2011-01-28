/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package LogServices;
import java.io.*;
import java.util.*;

/**
 *
 * @author FarmerLuo
 */
public class FileList {
    public File [] FileLists;

    public FileList( String path, String ext) {
        final String extend_name = ext;

        File dir = new File(path);
        FileLists = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.endsWith(extend_name)) {
                    return true;
                }
                return false;
            }
        });

        //对文件列表按修改时间排序
        Arrays.sort(FileLists, new Comparator<File>(){
            public int compare(File f1, File f2)
            {
                return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
            }
        });
    }

}
