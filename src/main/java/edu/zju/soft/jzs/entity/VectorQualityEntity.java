package edu.zju.soft.jzs.entity;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 11544 on 2017/11/17.
 */
public class VectorQualityEntity implements WritableComparable {

    private Text value;
    private LongWritable X;
    private LongWritable Y;
    private LongWritable times;

    public Text getValue() {
        return value;
    }

    public void setValue(Text value) {
        this.value = value;
    }

    public LongWritable getX() {
        return X;
    }

    public void setX(LongWritable x) {
        X = x;
    }

    public LongWritable getY() {
        return Y;
    }

    public void setY(LongWritable y) {
        Y = y;
    }

    public LongWritable getTimes() {
        return times;
    }

    public void setTimes(LongWritable times) {
        this.times = times;
    }


    public int compareTo(Object o) {
       if(o instanceof VectorQualityEntity)
       {
           VectorQualityEntity data=(VectorQualityEntity)o;
           if(this.getX().get()>data.getX().get())
           {
               return -1;
           }
           else if(this.getX().get()==data.getX().get())
           {
               return 0;
           }else
           {
               return 1;
           }
       }
       else
       {
           return -1;
       }
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(value.toString());
        out.writeLong(X.get());
        out.writeLong(Y.get());
        out.writeLong(times.get());
    }


    public void readFields(DataInput in) throws IOException {
        this.setValue(new Text(in.readUTF()));
        this.setX(new LongWritable(in.readLong()));
        this.setY(new LongWritable(in.readLong()));
        this.setTimes(new LongWritable(in.readLong()));
    }

    /**
     * 与HashCode一起重写，
     * 主要适用于在set时候进行判定
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VectorQualityEntity)) return false;

        VectorQualityEntity entity = (VectorQualityEntity) o;

        return value != null ? value.equals(entity.value) : entity.value == null;
    }

    /**
     * 与equals一起重写，主要适用于在set时候进行判定
     *
     * @return
     */
    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}
