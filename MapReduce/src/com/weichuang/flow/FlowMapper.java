package com.weichuang.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.转化文件
        String line = value.toString();
        //2.拆分文件
        String[] words = line.split("\t");
        //3.读文件
        Long up=Long.parseLong(words[words.length - 3]);
        Long down=Long.parseLong(words[words.length - 2]);
        FlowBean fb=new FlowBean(up,down);
        context.write(new Text(words[1]),fb);
        }
}
