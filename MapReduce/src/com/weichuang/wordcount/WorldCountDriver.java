package com.weichuang.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WorldCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置文件
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        //2.指定jar包所在位置
        job.setJarByClass(WorldCountDriver.class);
        //3.指定job需要哪个mapper/reduce
        job.setMapperClass(WorldCountMapper.class);
        job.setReducerClass(WorldCountReduce.class);
        //4.指定mapper输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.指定最终输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.将job之配置相关参数，以及job所用的java类所在的jar包交给yarn去运行
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
