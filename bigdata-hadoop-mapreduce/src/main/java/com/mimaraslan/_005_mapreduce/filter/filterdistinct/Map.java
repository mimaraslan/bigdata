package com.mimaraslan._005_mapreduce.filter.filterdistinct;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
    
    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

        String line = value.toString();
        String[] data = line.split("\t");
        
        // We don't need the value, only the distinct keys
        context.write(new Text(data[1]),NullWritable.get());

    }
}