package me.bigchunk.ex.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * vladvlaskin | 8/29/12/12:34 PM
 */

public class DFSUtil {

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
