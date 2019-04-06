package com.mimaraslan._003_mapreduce.combaine;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, NumPair, Text, DoubleWritable> {

    @Override
    public void reduce(final Text key, final Iterable<NumPair> values, final Context context)
            throws IOException, InterruptedException {
        Double sum = 0.0;
        Integer count = 0;

        for (NumPair value : values) {
            sum += value.getFirst().get();
            count += value.getSecond().get();
        };

        Double ratio = sum / count;

        context.write(key, new DoubleWritable(ratio));
    }
}