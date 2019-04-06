package com.mimaraslan._003_mapreduce.combaine;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, NumPair> {

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
            String maritalStatus = data[5];
            Double hrs = Double.parseDouble(data[12]);

            context.write(new Text(maritalStatus), new NumPair(hrs, 1));

        } catch (Exception e) {
            // Ignore any data which does not have these values.
        }
    }
}