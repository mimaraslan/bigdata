package com.mimaraslan._001_mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Split line into individual fields.
        // Each field is comma-separated from others.
        String line = value.toString();
        String[] data = line.split(",");

        // Pick the data columns marital status (col 5), hrs/wk (col 12)
        // Enclose in try in case of data issues ie missing values, corrupt rows etc
        try {
        // 34, Private, 226872, Bachelors, 13, Divorced, Sales, Not-in-family, White, Female, 0, 0, 40, United-States, <=50K
            String maritalStatus = data[5];
            Double hrs = Double.parseDouble(data[12]);
            context.write(new Text(maritalStatus), new DoubleWritable(hrs));
        } catch (Exception e) {
            // Ignore any data which does not have these values.
            e.printStackTrace();
        }

    }
}