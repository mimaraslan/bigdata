package com.mimaraslan._005_mapreduce.filter.filterdistinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    public void reduce(final Text key,
                       final Iterable<NullWritable> values,
                       final Context context)
    throws IOException, InterruptedException {

        // The framework takes care of collecting all
        // values with the same key so each key will be
        // present only once
        context.write(key, NullWritable.get());
    }
}