package com.sxt.hadoop.hdfs;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class TestHDFS {

    Configuration conf;
    FileSystem fs;


    @Before
    public void conn() throws IOException {

        conf = new Configuration(true);
        fs = FileSystem.get(conf);

    }

    @After
    public void close() throws Exception {
        fs.close();
    }

    @Test
    public void mkdir() throws Exception {

        Path ifile = new Path("/ooxx");
        if(fs.exists(ifile)){
            fs.delete(ifile, true);
        }
        fs.mkdirs(ifile);

    }

    @Test
    public void upload() throws IOException {

        Path fLocal = new Path("/ooxx/19Q1-交通费查验.pdf");
        FSDataOutputStream outputStream = fs.create(fLocal);

        InputStream inputStream = new BufferedInputStream(new FileInputStream(new File("C:\\Users\\刘伟\\Desktop\\19Q1-交通费查验.pdf")));

        IOUtils.copyBytes(inputStream, outputStream, conf,true);

    }


    @Test
    public void blks() throws IOException {

        Path i = new Path("/user/root/test.txt");
        FileStatus ifile = fs.getFileStatus(i);
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(ifile, 0, ifile.getLen());

        for (BlockLocation blockLocation : fileBlockLocations) {
            System.out.println(blockLocation);
        }

        FSDataInputStream in = fs.open(i);
        System.out.println((char) in.readByte());


    }



}
