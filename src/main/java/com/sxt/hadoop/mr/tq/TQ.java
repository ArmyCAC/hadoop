package com.sxt.hadoop.mr.tq;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TQ implements WritableComparable<TQ> {

    private int year;
    private int mouth;
    private int day;
    private int wd;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMouth() {
        return mouth;
    }

    public void setMouth(int mouth) {
        this.mouth = mouth;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    @Override
    public int compareTo(TQ that) {
        int c1 = Integer.compare(this.year, that.getYear());
        if (c1 == 0) {
            int c2 = Integer.compare(this.mouth, that.getMouth());
            if (c2 == 0) {
                return Integer.compare(this.day, that.getDay());
            }
            return c2;
        }
        return c1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(mouth);
        out.writeInt(day);
        out.writeInt(wd);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.mouth = in.readInt();
        this.day = in.readInt();
        this.wd = in.readInt();
    }
}
