package com.mimaraslan._004_mapreduce.filter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] data = line.split("\t");

        // 1	Murat	Books	200
        if (data[2].equalsIgnoreCase("Books")){
            context.write(NullWritable.get(), value);
        }

    }
}