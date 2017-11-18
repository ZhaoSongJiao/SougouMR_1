package edu.zju.soft.jzs.entity;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by 11544 on 2017/11/17.
 */
public class CircleCaculate implements WritableComparable{

    private VectorQualityEntity thecenter;

    //使用Set进行，防止数据量过大
    private Set<VectorQualityEntity> theflower;

    private Set<String> outputText;

    private long times=1;

    private long startX;
    private long startY;

    private long newX;
    private long newY;

    public CircleCaculate(VectorQualityEntity thecenter)
    {

        theflower=new LinkedHashSet<VectorQualityEntity>();
        startX=thecenter.getX().get();
        startY=thecenter.getY().get();
        this.setThecenter(thecenter);
        newX=getThecenter().getX().get();
        newY=getThecenter().getY().get();
        outputText=new LinkedHashSet<String>();
    }


    public VectorQualityEntity getThecenter() {
        return thecenter;
    }


    public void setThecenter(VectorQualityEntity thecenter) {
        this.thecenter = thecenter;
    }


    public Set<VectorQualityEntity> getTheflower() {
        return theflower;
    }

    public void setTheflower(Set<VectorQualityEntity> theflower) {
        this.theflower = theflower;
    }

    public long getTimes() {
        return times;
    }

    public long getNewX() {
        return newX;
    }

    public long getNewY() {
        return newY;
    }


    /**
     * 首先你先判断最短距离的
     * @param data
     * @return
     */
    public long getDistanceToThisCenter(VectorQualityEntity data)
    {
        long xcount=Math.abs(data.getX().get()-thecenter.getX().get());
//        long ycount=data.getY().get()-thecenter.getY().get();
        return xcount;
    }


    /**
     * 添加一个新的数据并动态的调整数据中心点
     * 动态的调整中心点
     * @param vector
     * @return
     */
    public boolean addNewFlower(VectorQualityEntity vector)
    {
        try
        {
            long alloldx=this.getNewX()*times;
//            long alloldy=this.getNewY()*times;
            this.newX=(alloldx+vector.getX().get())/(times+1);
//            this.newY=(alloldy+vector.getY().get())/(times+1);
            long distance=newX-startX;
            long maxdistance= (long) (startX*0.01);
            //重新回归计算点
            if(distance>=maxdistance)
            {
                this.newX= (long) ((this.startX+this.newX)*0.5);
//                this.newY= (long) ((this.startY+this.newY)*0.5);
            }
            this.times++;
            this.getTheflower().add(vector);
        }
        catch (Exception ex)
        {
            return false;
        }
        return true;
    }

    /**
     * 执行结束并返回一个新的中心点
     * @return
     */
    public VectorQualityEntity endAndBackTheNewCenter()
    {
        VectorQualityEntity entity=new VectorQualityEntity();
        entity.setX(new LongWritable(newX));
        entity.setY(new LongWritable(newY));
        entity.setValue(thecenter.getValue());
        return entity;
    }


    public int compareTo(Object o) {
        if(o instanceof CircleCaculate)
        {
            CircleCaculate data=(CircleCaculate) o;
            if (theflower.size()>data.theflower.size())
            {
                return -1;
            }
            else if(theflower.size()==data.theflower.size())
            {
                return 0;
            }
            else
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
        out.writeUTF(this.thecenter.getValue().toString());
        out.writeLong(startX);
        out.writeLong(startY);
        out.writeLong(newX);
        out.writeLong(newY);
        out.writeLong(times);
    }

    public void readFields(DataInput in) throws IOException {
        String value=in.readUTF();
        this.startX=in.readLong();
        this.startY=in.readLong();
        this.newX=in.readLong();
        this.newY=in.readLong();
        this.times=in.readLong();
        this.setTheflower(new LinkedHashSet<VectorQualityEntity>());
        VectorQualityEntity center=new VectorQualityEntity();
        center.setValue(new Text(value));
        center.setX(new LongWritable(this.startX));
        center.setY(new LongWritable(this.startY));
        center.setTimes(new LongWritable(this.times));
        this.setThecenter(center);
    }

    @Override
    public String toString()
    {
        StringBuffer buffer=new StringBuffer();
        buffer.append(this.thecenter.getValue().toString()+"\t");
        buffer.append(this.startX+"\t");
        buffer.append(this.startY+"\t");
        buffer.append(this.newX+"\t") ;
        buffer.append(this.newY+"\t");
        buffer.append(this.times+"\t");
        for(VectorQualityEntity entity:this.theflower)
        {
            outputText.add(entity.getValue().toString());
        }


        buffer.append("flowerstext:{|{");
        for(String entity:outputText)
        {
            buffer.append(entity);
            buffer.append("|,|");
        }
        buffer.append("}|}");
        return buffer.toString();
    }

}
