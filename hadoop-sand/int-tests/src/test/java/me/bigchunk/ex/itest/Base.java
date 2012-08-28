package me.bigchunk.ex.itest;

import me.bigchunk.ex.mr.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;

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

        mrCluster = new MiniMRCluster(1, getFileSystem().getUri().toString(), 1);
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

    private static FileSystem getFileSystem() throws IOException {
        return dfsCluster.getFileSystem();
    }

    private static Configuration getConfiguration() throws Exception {
        return getFileSystem().getConf();
    }

    private void createTextInputFile() throws Exception {
        OutputStream outputStream = getFileSystem().create(new Path(input, "wordcount"));
        Writer writer = new OutputStreamWriter(outputStream);
        writer.write("1 2 3\n3 2 1\n 3 1 2");
        IOUtils.closeStream(writer);
    }

    private void stdoutFile(Path path) throws IOException {
        InputStream is = getFileSystem().open(path);
        StringWriter writer = new StringWriter();
    }

    @Test
    public void testSeqFileReading() throws Exception {
        createTextInputFile();
        assertTrue(getFileSystem().exists(new Path(input, "wordcount")));
        assertFalse(getFileSystem().exists(new Path("foo")));
    }

    @Test
    public void testWordCountMR() throws Exception {
        createTextInputFile();
        assertTrue(getJob().waitForCompletion(true));
    }

    private Job getJob() throws Exception {

        Job job = new Job(getConfiguration());

        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCount.Map.class);
        job.setReducerClass(WordCount.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }


}
