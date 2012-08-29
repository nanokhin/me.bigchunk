package me.bigchunk.ex.utils;

import com.google.protobuf.DescriptorProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Options;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * vladvlaskin | 8/29/12/12:34 PM
 */

public class DFSUtil {

    public static <K extends Writable, V extends Writable>
    void appendItemsToSequenceFile(Configuration conf,
                                   Path path,
                                   Class<K> keyClass,
                                   Class<V> valClass,
                                   Iterator<? extends Map.Entry<K, V>> it)
            throws IOException {
        SequenceFile.Writer writer = null;
        try {
            writer = getSeqFileWriter(conf, path, keyClass, valClass);
            while (it.hasNext()) {
                Map.Entry<K, V> e = it.next();
                writer.append(e.getKey(), e.getValue());
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    public static <K extends Writable, V extends Writable> SequenceFile.Writer getSeqFileWriter(Configuration conf,
                                                                                                Path path,
                                                                                                Class<K> keyClass,
                                                                                                Class<V> valueClass
    ) throws IOException {
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(keyClass),
                SequenceFile.Writer.valueClass(valueClass),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD)
        );
        return writer;
    }

    public static <K extends Writable, V extends Writable> List<Map.Entry<K, V>>
    readSeqFile(Configuration conf, Path path) throws IOException, IllegalAccessException, InstantiationException {

        SequenceFile.Reader reader = null;
        List<Map.Entry<K, V>> list = new ArrayList<Map.Entry<K, V>>();
        try {
            reader = getSeqFileReader(conf, path);
            K key = (K) reader.getKeyClass().newInstance();
            V value = (V) reader.getValueClass().newInstance();

            while (reader.next(key, value)) {
                Map.Entry<K, V> e = new MeEntry<K, V>(key, value);
                list.add(e);
                key = (K) reader.getKeyClass().newInstance();
                value = (V) reader.getValueClass().newInstance();
            }
            return list;
        } catch (IOException e) {
            throw e;
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    public static SequenceFile.Reader getSeqFileReader(Configuration conf, Path path) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
        return reader;
    }

    public static String getFileContent(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path))
            throw new FileNotFoundException(String.format("File at path %s was not founded", path.toString()));
        if (!fs.isFile(path))
            throw new IOException(String.format("Only file undere the path is accepted, path %s links no to a file", path));

        InputStream is = fs.open(path);
        StringWriter writer = new StringWriter();
        org.apache.commons.io.IOUtils.copy(is, writer);
        return writer.toString();
    }

    public static void writeToFile(FileSystem fs, Path path, boolean override, String... lines) throws IOException {
        OutputStream outputStream = fs.create(path, override);
        Writer writer = new OutputStreamWriter(outputStream);
        for (String ln : lines) {
            writer.write(ln);
        }
        IOUtils.closeStream(writer);
    }
}
