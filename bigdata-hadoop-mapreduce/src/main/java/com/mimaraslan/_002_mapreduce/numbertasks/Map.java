package com.mimaraslan._002_mapreduce.numbertasks;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The Mapper class for ViewCount. It outputs key value pairs of the form (member,1)
 */
public class Map extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split("\t");
        context.write(new Text(row[2]),new IntWritable(1));
    }
}


