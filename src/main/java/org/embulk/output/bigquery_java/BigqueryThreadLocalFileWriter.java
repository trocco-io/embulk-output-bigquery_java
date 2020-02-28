package org.embulk.output.bigquery_java;

import java.util.concurrent.ConcurrentHashMap;

import org.embulk.output.bigquery_java.config.PluginTask;


public class BigqueryThreadLocalFileWriter {
    private static ThreadLocal<BigqueryFileWriter> tl = ThreadLocal.withInitial(BigqueryFileWriter::new);

    public static void setFileWriter(PluginTask task){
        BigqueryFileWriter writer = tl.get();
        writer.setTask(task);
        writer.setCompression(task.getCompression());
        ConcurrentHashMap<Long, BigqueryFileWriter> writers = BigqueryUtil.getFileWriters();
        writers.put(Thread.currentThread().getId(), writer);
        tl.set(writer);
        tl.remove();
    }

    public static void write(byte[] bytes){
        tl.get().write(bytes);
    }
}