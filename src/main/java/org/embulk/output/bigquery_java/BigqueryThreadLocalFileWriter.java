package org.embulk.output.bigquery_java;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.embulk.output.bigquery_java.config.PluginTask;


public class BigqueryThreadLocalFileWriter {
    private static ThreadLocal<BigqueryFileWriter> tl = ThreadLocal.withInitial(BigqueryFileWriter::new);
    private static ConcurrentHashMap<Long, BigqueryFileWriter> writers;

    public static void setFileWriter(PluginTask task){
        BigqueryFileWriter writer = tl.get();
        long key = Thread.currentThread().getId();
        writers = BigqueryUtil.getFileWriters();
        if (!writers.containsKey(key)){
            writer.setTask(task);
            writer.setCompression(task.getCompression());
            writers.put(key, writer);
            tl.set(writer);
        }
    }

    public static void write(byte[] bytes){
        tl.get().write(bytes);
    }

    public static void remove(){
        tl.remove();
    }

}