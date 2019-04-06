package com.mimaraslan._006_mapreduce.filter.topn;

import org.apache.hadoop.io.Text;

public class User implements Comparable<User> {
    
    private int followers;
    private Text record;
    
    public User(int followers, Text record) {
        this.followers = followers;
        this.record = record;
    }
    
    @Override
    public int compareTo(User user) {
        return this.followers - user.followers;
    }
    
    public int getFollowers() {
        return followers;
    }
    
    public Text getRecord() {
        return record;
    }
}
