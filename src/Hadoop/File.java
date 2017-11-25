package Hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.net.URI;

public class File {
    public static void read() throws Exception {
        String uri = "hdfs://localhost:9000/spark.txt";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;

        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);

            System.out.println("End Of file: HDFS file read complete");

        } finally {
            IOUtils.closeStream(in);
        }
    }
}
