package com.mimaraslan._004_mapreduce.filter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {
    
    @Override
    public void reduce(final NullWritable key,
                       final Iterable<Text> values,
                       final Context context)
    throws IOException, InterruptedException {

        // Identity function
        for (Text value : values) {
            context.write(NullWritable.get(), value);
        }

    }
}
