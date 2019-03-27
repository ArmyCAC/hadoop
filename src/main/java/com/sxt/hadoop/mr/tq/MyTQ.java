package com.sxt.hadoop.mr.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyTQ {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        job.setJarByClass(MyTQ.class);

        //------conf-------------
        //------MAP:
//        job.setInputFormatClass(ooxx.class);

        job.setMapperClass(TMapper.class);
        job.setMapOutputKeyClass(TQ.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setPartitionerClass(TPartitioner.class);
        job.setSortComparatorClass(TSortComparator.class);

//        job.setCombinerClass(TCombiner.class);
        //-----MAP-END

        //-----REDUCE:
        job.setGroupingComparatorClass(TGroupingComparator.class);
        job.setReducerClass(TReducer.class);

        //-----REDUCE-END

        Path input = new Path("/data/tq/input");
        FileInputFormat.addInputPath(job, input);

        Path output = new Path("/data/tq/output");
        FileOutputFormat.setOutputPath(job, output);

        job.setNumReduceTasks(2);

        job.waitForCompletion(true);



    }




}
