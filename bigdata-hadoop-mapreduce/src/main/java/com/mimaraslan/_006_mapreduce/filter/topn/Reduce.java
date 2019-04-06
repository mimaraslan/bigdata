package com.mimaraslan._006_mapreduce.filter.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.PriorityQueue;

public class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {
    
    private PriorityQueue<User> followersPriorityQueue = new PriorityQueue<>();
    
    @Override
    public void reduce(NullWritable key,
                       Iterable<Text> values,
                       Context context)
    throws IOException, InterruptedException {

        for (Text value : values) {

            String line = value.toString();
            String[] data = line.split("\t");

                                  //       2	30000
            int followers = Integer.parseInt(data[1]);
            
            User user = followersPriorityQueue.peek();
            if (followersPriorityQueue.size() <= 3 || followers > user.getFollowers()) {
                followersPriorityQueue.add(new User(followers, new Text(value)));
                
                if (followersPriorityQueue.size() > 3) {
                    followersPriorityQueue.remove(user);
                }
            }
        }
        
        while (!followersPriorityQueue.isEmpty()) {
            context.write(NullWritable.get(),
                          followersPriorityQueue.poll().getRecord());
        }

    }
}








