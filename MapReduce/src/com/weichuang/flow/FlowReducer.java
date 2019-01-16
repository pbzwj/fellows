package com.weichuang.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int up=0;
        int down=0;
        for (FlowBean fb:values) {
            up+=fb.getUpFlow();
            down+=fb.getDownFlow();
        }
        context.write(new Text(key),new FlowBean(up,down));
    }
}
