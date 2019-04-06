package com.mimaraslan._006_mapreduce.filter.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.PriorityQueue;

public  class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
    
    private PriorityQueue<User> followersPriorityQueue = new PriorityQueue<>();
    
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] data = line.split("\t");

        //  2	30000
        Integer followers = Integer.parseInt(data[1]);
        
        User user = followersPriorityQueue.peek();
        if (followersPriorityQueue.size() <= 3 || followers > user.getFollowers()) {
            followersPriorityQueue.add(new User(followers, new Text(value)));
            
            if (followersPriorityQueue.size() > 3) {
                followersPriorityQueue.poll();
            }
        }
    }
    
    protected void cleanup(Context context) throws IOException,
    InterruptedException {
        while (!followersPriorityQueue.isEmpty()) {
            context.write(NullWritable.get(),followersPriorityQueue.poll().getRecord());
        }
    }
}








