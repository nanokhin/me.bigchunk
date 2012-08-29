package me.bigchunk.ex.test;

import me.bigchunk.ex.mr.WordCount;
import me.bigchunk.ex.utils.DFSUtil;
import me.bigchunk.ex.utils.MRUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * vladvlaskin | 8/27/12/6:49 PM
 */
public class Base {

    private static MiniDFSCluster dfsCluster = null;
    private static MiniMRCluster mrCluster = null;

    private static final Path input = new Path("input");
    private static final Path output = new Path("output");

    @BeforeClass
    public static void setUp() throws Exception {

        new File("test-logs").mkdirs();
        System.setProperty("hadoop.log.dir", "test-logs");

        Configuration conf = new Configuration();
        MiniDFSCluster.Builder b = new MiniDFSCluster.Builder(conf);
        dfsCluster = b.build();
        dfsCluster.getFileSystem().makeQualified(input);
        dfsCluster.getFileSystem().makeQualified(output);
        mrCluster = new MiniMRCluster(1, getFS().getUri().toString(), 1);
    }

    @Test
    public void testWordCountMR() throws Exception {

        //INIT
        Path inputPath = new Path(input, "wordCount");
        Path resultPath = new Path("testMapReduceResult");

        String[] content = new String[]
                        {"1 2 3\n",
                        "2 3 4\n",
                        "4 5 6 6\n"};
        DFSUtil.writeToFile(getFS(), inputPath, true, content);

        Job job = MRUtil.setUpJob(getConf(),
                WordCount.class, WordCount.Map.class, WordCount.Reduce.class,
                Text.class, IntWritable.class, input, output);

        //ACT & ASSERT
        boolean jobResult = job.waitForCompletion(true);

        assertTrue(jobResult);
        FileUtil.copyMerge(getFS(), output, getFS(), resultPath, false, getConf(), "\n");
        String result = DFSUtil.getFileContent(getFS(), resultPath);

        Pattern p = Pattern.compile("[\\s]+");
        for(String ln : result.split("\n")){
            String[] splitty = p.split(ln.trim());
            if (splitty.length == 2){
                int term = new Integer(splitty[0]);
                int freq = new Integer(splitty[1]);
                switch (term) {
                    case 1: assertEquals(freq, 1); break;
                    case 2: assertEquals(freq, 2); break;
                    case 3: assertEquals(freq, 2); break;
                    case 4: assertEquals(freq, 2); break;
                    case 5: assertEquals(freq, 1); break;
                    case 6: assertEquals(freq, 2); break;
                    default: throw new Exception("Unknown term " + term);
                }
            }
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (dfsCluster != null) {
            dfsCluster.shutdown();
        }
        if (mrCluster != null) {
            mrCluster.shutdown();
        }
    }

    private static FileSystem getFS() throws IOException {
        return dfsCluster.getFileSystem();
    }

    private static Configuration getConf() throws Exception {
        return getFS().getConf();
    }
}
