package me.bigchunk.ex.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * vladvlaskin | 8/29/12/1:29 PM
 */
public class MRUtil {

    public static Job setUpJob(Configuration conf,
                               Class jarClass,
                               Class<? extends Mapper> mapClass,
                               Class<? extends Reducer> reduceClass,
                               Class outKeyClass,
                               Class outValClass,
                               Path inputPath,
                               Path outputPath)
            throws IOException {
        Job job = new Job(conf);
        job.setJarByClass(jarClass);
        job.setMapperClass(mapClass);
        job.setReducerClass(reduceClass);
        job.setOutputKeyClass(outKeyClass);
        job.setOutputValueClass(outValClass);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

}
