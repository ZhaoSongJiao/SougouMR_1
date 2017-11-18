package edu.zju.soft.jzs.entity;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 11544 on 2017/11/15.
 */
public class SoGouData implements WritableComparable {

    private Text ts;
    private Text uid;
    private Text keyword;
    private IntWritable rank;
    private IntWritable order;
    private Text url;
    private IntWritable year;
    private IntWritable month;
    private IntWritable day;
    private IntWritable hour;
    private IntWritable minutes;
    private IntWritable second;

    public Text getTs() {
        return ts;
    }

    public void setTs(Text ts) {
        this.ts = ts;
    }

    public Text getUid() {
        return uid;
    }

    public void setUid(Text uid) {
        this.uid = uid;
    }

    public Text getKeyword() {
        return keyword;
    }

    public void setKeyword(Text keyword) {
        this.keyword = keyword;
    }

    public IntWritable getRank() {
        return rank;
    }

    public void setRank(IntWritable rank) {
        this.rank = rank;
    }

    public IntWritable getOrder() {
        return order;
    }

    public void setOrder(IntWritable order) {
        this.order = order;
    }

    public Text getUrl() {
        return url;
    }

    public void setUrl(Text url) {
        this.url = url;
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
    }

    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    public IntWritable getDay() {
        return day;
    }

    public void setDay(IntWritable day) {
        this.day = day;
    }

    public IntWritable getHour() {
        return hour;
    }

    public void setHour(IntWritable hour) {
        this.hour = hour;
    }

    public IntWritable getMinutes() {
        return minutes;
    }

    public void setMinutes(IntWritable minutes) {
        this.minutes = minutes;
    }

    public IntWritable getSecond() {
        return second;
    }

    public void setSecond(IntWritable second) {
        this.second = second;
    }


    public int compareTo(Object o) {
        if(o==null)
        {
            return -1;
        }
        else if(o instanceof SoGouData)
        {
            SoGouData data=(SoGouData) o;
            if(data.ts.equals(this.getTs()))
            {
                if(data.uid.equals(this.getUid()))
                {
                    if(data.getKeyword().equals(this.getKeyword()))
                    {
                        return 0;
                    }
                    else
                    {
                        return -1;
                    }
                }
                else
                {
                    return -1;
                }
            }
            else
            {
                return -1;
            }
        }
        else
        {
            return -1;
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(ts.toString());
        out.writeUTF(uid.toString());
        out.writeUTF(keyword.toString());
        out.writeInt(order.get());
        out.writeInt(rank.get());
        out.writeUTF(url.toString());
        out.writeInt(year.get());
        out.writeInt(month.get());
        out.writeInt(day.get());
        out.writeInt(hour.get());
        out.writeInt(minutes.get());
        out.writeInt(second.get());
    }

    public void readFields(DataInput in) throws IOException {
        this.setTs(new Text(in.readUTF()));
        this.setUid(new Text(in.readUTF()));
        this.setKeyword(new Text(in.readUTF()));
        this.setRank(new IntWritable(in.readInt()));
        this.setOrder(new IntWritable(in.readInt()));
        this.setUrl(new Text(in.readUTF()));
        this.setYear(new IntWritable(in.readInt()));
        this.setMonth(new IntWritable(in.readInt()));
        this.setDay(new IntWritable(in.readInt()));
        this.setHour(new IntWritable(in.readInt()));
        this.setMinutes(new IntWritable(in.readInt()));
        this.setSecond(new IntWritable(in.readInt()));
    }

    /**
     * @return
     */
    public String toString()
    {
        String str=new String();
        str=str+keyword;
        return str;
    }

}
