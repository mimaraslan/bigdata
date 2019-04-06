package com.mimaraslan._008_mapreduce.filter.withcombiner;

/**
 * A Custom WritableComparable class to represent a pair of numbers
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumPair implements WritableComparable<NumPair>{

    private DoubleWritable first;
    private IntWritable second;

    public NumPair(){
        set(new DoubleWritable(),new IntWritable());
    }

    public NumPair(Double first, Integer second) {
        set(new DoubleWritable(first),new IntWritable(second));
    }

    public NumPair(DoubleWritable first, IntWritable second) {
        set(first, second);
    }

    private void set(DoubleWritable first, IntWritable second){
        this.first = first;
        this.second = second;
    }

    /**
     * @return the first of the pair, representing the hours worked per week
     */
    public DoubleWritable getFirst() {
        return first;
    }

    /**
     * @return the second of the pair, the count of number of people
     */
    public IntWritable getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException{
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int compareTo(NumPair numPair){
        int cmp = first.compareTo(numPair.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(numPair.second);
    }

    @Override
    public int hashCode() {
        return first.hashCode()*163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o){
        if (o instanceof NumPair) {
            NumPair numPair = (NumPair) o;
            return first.equals(numPair.first) && second.equals(numPair.second);
        }

        return  false;
    }

    @Override
    public String toString(){
        return first +"\t" +second;
    }
}
