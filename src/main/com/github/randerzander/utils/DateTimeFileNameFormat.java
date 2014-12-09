package com.github.randerzander.utils;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import backtype.storm.task.TopologyContext;

import java.util.Map;
import java.util.Date;
import java.text.Format;
import java.text.SimpleDateFormat;

public class DateTimeFileNameFormat implements FileNameFormat {
    private String componentId;
    private int taskId;
    private String path = "/storm";
    private String prefix = "";
    private String extension = ".txt";

    public DateTimeFileNameFormat withPrefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    public DateTimeFileNameFormat withExtension(String extension){
        this.extension = extension;
        return this;
    }

    public DateTimeFileNameFormat withPath(String path){
        this.path = path;
        return this;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext) {
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();
    }

    public String convertTime(long time){
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        return format.format(date);
    }

    @Override
    public String getName(long rotation, long timeStamp) {
        return this.prefix + this.componentId + "-" + this.taskId +  "-" + rotation + "-" + convertTime(timeStamp) + this.extension;
    }

    public String getPath(){
        return this.path;
    }
}
