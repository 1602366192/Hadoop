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

    public static void loc() throws Exception {
        String uri = "hdfs://localhost:9000/spark.txt";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        FileStatus filestatus = fs.getFileStatus(new Path(uri));
        System.out.println(filestatus);

        BlockLocation[] blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());

        int blockLen = blkLocations.length;

        for(int i=0;i<blockLen;i++){
            String[] hosts = blkLocations[i].getHosts();
            System.out.println("block_"+i+"_location:"+hosts[0]);
        }

    }

}
