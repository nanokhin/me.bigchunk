package me.bigchunk.ex.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;

import java.io.File;
import java.io.IOException;

/**
 * vladvlaskin | 8/29/12/10:24 PM
 */
public class SandBox {
    private MiniDFSCluster dfsCluster = null;
    private MiniMRCluster mrCluster = null;
    private Path input = new Path("input");
    private Path output = new Path("output");
    private boolean initiated = false;

    private static SandBox instance = null;

    public void initCluster() throws Exception {
        if (!initiated) {
            new File("test-logs").mkdirs();
            System.setProperty("hadoop.log.dir", "test-logs");

            Configuration conf = new Configuration();
            MiniDFSCluster.Builder b = new MiniDFSCluster.Builder(conf);
            dfsCluster = b.build();
            dfsCluster.getFileSystem().makeQualified(input);
            dfsCluster.getFileSystem().makeQualified(output);
            mrCluster = new MiniMRCluster(1, getFS().getUri().toString(), 1);
            initiated = true;
        }
    }

    public void dropCluster() throws Exception {
        if (initiated) {
            if (dfsCluster != null) {
                dfsCluster.shutdown();
            }
            if (mrCluster != null) {
                mrCluster.shutdown();
            }
            initiated = false;
        }
    }

    public static SandBox getInstance() {
        synchronized (SandBox.class) {
            if (instance == null) {
                instance = new SandBox();
            }
        }
        return instance;
    }

    private SandBox() {
    }

    public FileSystem getFS() throws IOException {
        return getInstance().getDfsCluster().getFileSystem();
    }

    public Configuration getConf() throws Exception {
        return getFS().getConf();
    }

    public MiniDFSCluster getDfsCluster() {
        return dfsCluster;
    }

    public MiniMRCluster getMrCluster() {
        return mrCluster;
    }

    public Path getInputPath() {
        return input;
    }

    public Path getOutputPath() {
        return output;
    }
}
