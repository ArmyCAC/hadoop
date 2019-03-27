package com.sxt.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyWC {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1、读取配置文件
        Configuration conf = new Configuration(true);

        // Create a new Job
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyWC.class);

        // Specify various job-specific parameters
        job.setJobName("ooxx");

//        job.setInputPath(new Path("in"));
//        job.setOutputPath(new Path("out"));

        Path input = new Path("/user/root/test.txt");
        FileInputFormat.addInputPath(job, input);

        Path output = new Path("/data/wc/output");
        if (output.getFileSystem(conf).exists(output)) {
            output.getFileSystem(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);





    }


}
