package com.mimaraslan._007_mapreduce.filter.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, Text,Text,Text> {
    @Override
    public void reduce(final Text key,
                       final Iterable<Text> values,
                       final Context context)
    throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        
        for (Text value : values) {
            stringBuilder.append(value.toString());
            
            if (values.iterator().hasNext()) {
                stringBuilder.append(" | ");
            }
        }
        
        context.write(key, new Text(stringBuilder.toString()));
    }
}