package com.weichuang.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WorldCountReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.汇总key的个数
        int count=0;
        for (IntWritable val:values) {
            count+=val.get();
        }
        //输出
        context.write(new Text(key),new IntWritable(count));
    }
}
