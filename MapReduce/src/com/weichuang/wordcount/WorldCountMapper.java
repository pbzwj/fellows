package com.weichuang.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WorldCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.转化类型
        String line = value.toString();
        //2.根据空格分割单词
        String[] words = line.split(" ");
        //3.将单词输出
        for (String word: words) {
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
