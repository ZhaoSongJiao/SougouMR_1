package edu.zju.soft.jzs.entity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 11544 on 2017/11/15.
 */
public class CountEntity implements WritableComparable {

    private Text value;
    private long times;

    public Text getValue() {
        return value;
    }

    public void setValue(Text value) {
        this.value = value;
    }

    public long getTimes() {
        return times;
    }

    public void setTimes(long times) {
        this.times = times;
    }


    public int compareTo(Object o) {
        CountEntity entity;
        if(o instanceof CountEntity)
        { entity=(CountEntity) o; }
        else
        { return -1;  }
        if(this.times>entity.times)
        { return -1; }
        else if(this.times<entity.times)
        { return 1; }
        else    return -1;
    }

    public void write(DataOutput out) throws IOException {

           // value.write(out);
            out.writeUTF(value.toString());
            out.writeLong(times);
    }

    public void readFields(DataInput in) throws IOException {
            this.setValue(new Text(in.readUTF()));
            this.setTimes(in.readLong());
    }
}
