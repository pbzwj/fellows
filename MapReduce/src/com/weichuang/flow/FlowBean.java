package com.weichuang.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private long upFlow;
    private long downFlow;
    private long totolFlow;
    public FlowBean(){
    }
    public FlowBean(long upFlow,long downFlow){
        this.upFlow=upFlow;
        this.downFlow=downFlow;
        this.totolFlow=upFlow+downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getTotolFlow() {
        return totolFlow;
    }

    public void setTotolFlow(long totolFlow) {
        this.totolFlow = totolFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.totolFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow=in.readLong();
        this.downFlow=in.readLong();
        this.totolFlow=in.readLong();
    }

    @Override
    public String toString() {
        return upFlow +"/t" + downFlow +"/t" + totolFlow +"/n";
    }
}
