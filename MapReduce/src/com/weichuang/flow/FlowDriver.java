package com.weichuang.flow;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.file.FileSystem;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置文件路径
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        //2.获取Jar包位置
        job.setJarByClass(FlowBean.class);
        //3.获取mapper和reducer位置
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //4.输出mapper
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.输出reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7.退出系统
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
