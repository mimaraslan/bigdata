package com.mimaraslan._009_mapreduce.filter.withoutcombiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(final Text key,
                       final Iterable<DoubleWritable> values,
                       final Context context)
            throws IOException, InterruptedException {
        Double sum = 0.0;
        Integer count = 0;

        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        };

        Double ratio = sum / count;
        context.write(key, new DoubleWritable(ratio));
    }
}