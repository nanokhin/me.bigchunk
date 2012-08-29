package me.bigchunk.ex.test;

import me.bigchunk.ex.mr.WordCount;
import me.bigchunk.ex.utils.DFSUtil;
import me.bigchunk.ex.utils.MRUtil;
import me.bigchunk.ex.utils.MeEntry;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * vladvlaskin | 8/27/12/6:49 PM
 */
public class WordCountTest {

    static SandBox box;

    static {
        box = box.getInstance();
    }

    @BeforeClass
    public static void init() throws Exception {
        box.initCluster();
    }

    @AfterClass
    public static void drop() throws Exception {
        box.dropCluster();
    }


    @Test
    public void testWordCountMR() throws Exception {
        //INIT
        Path inputPath = new Path(box.getInputPath(), "wordCount");
        Path resultPath = new Path("testMapReduceResult");

        String[] content = new String[]
                {"1 2 3\n",
                        "2 3 4\n",
                        "4 5 6 6\n"};
        DFSUtil.writeToFile(box.getFS(), inputPath, true, content);

        Job job = MRUtil.setUpJob(box.getConf(),
                WordCount.class, WordCount.Map.class, WordCount.Reduce.class,
                Text.class, IntWritable.class, box.getInputPath(), box.getOutputPath());

        //ACT & ASSERT
        boolean jobResult = job.waitForCompletion(true);

        assertTrue(jobResult);
        FileUtil.copyMerge(box.getFS(), box.getOutputPath(), box.getFS(), resultPath, false, box.getConf(), "\n");
        String result = DFSUtil.getFileContent(box.getFS(), resultPath);

        Pattern p = Pattern.compile("[\\s]+");
        for (String ln : result.split("\n")) {
            String[] splitty = p.split(ln.trim());
            if (splitty.length == 2) {
                int term = new Integer(splitty[0]);
                int freq = new Integer(splitty[1]);
                switch (term) {
                    case 1:
                        assertEquals(freq, 1);
                        break;
                    case 2:
                        assertEquals(freq, 2);
                        break;
                    case 3:
                        assertEquals(freq, 2);
                        break;
                    case 4:
                        assertEquals(freq, 2);
                        break;
                    case 5:
                        assertEquals(freq, 1);
                        break;
                    case 6:
                        assertEquals(freq, 2);
                        break;
                    default:
                        throw new Exception("Unknown term " + term);
                }
            }
        }
    }

    @Test
    public void testSeqFileWriter() throws Exception {
        int[] keys = {2, 3, 1};
        String[] vals = {"b", "c", "a"};
        Path testPath = new Path("testSeqFile");
        List<MeEntry<IntWritable, Text>> list = new ArrayList<MeEntry<IntWritable, Text>>();

        for (int i = 0; i < keys.length; i++) {
            MeEntry<IntWritable, Text> e = new MeEntry<IntWritable, Text>(new IntWritable(keys[i]), new Text(vals[i]));
            list.add(e);
        }

        DFSUtil.appendItemsToSequenceFile(box.getConf(), testPath, IntWritable.class, Text.class, list.iterator());

        List<Map.Entry<IntWritable, Text>> readedVals = DFSUtil.readSeqFile(box.getConf(), testPath);

        for (Map.Entry<IntWritable, Text> read : readedVals) {
            int key = read.getKey().get();
            String val = new String(read.getValue().getBytes());
            switch (key) {
                case 1:
                    assertEquals(val, "a");
                    break;
                case 2:
                    assertEquals(val, "b");
                    break;
                case 3:
                    assertEquals(val, "c");
                    break;
                default:
                    throw new Exception("Unknown key: " + key);
            }
        }
    }
}
