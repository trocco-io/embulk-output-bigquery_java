package org.embulk.output.bigquery_java;

import java.util.concurrent.ConcurrentHashMap;

import org.embulk.output.bigquery_java.config.PluginTask;


public class BigqueryThreadLocalFileWriter {
    private static ThreadLocal<BigqueryFileWriter> tl = ThreadLocal.withInitial(BigqueryFileWriter::new);
    private static ConcurrentHashMap<Long, BigqueryFileWriter> writers;

    public static void setFileWriter(PluginTask task) {
        BigqueryFileWriter writer = tl.get();
        writer.setTask(task);
        writer.setCompression(task.getCompression());
        writers = BigqueryUtil.getFileWriters();
        writers.put(Thread.currentThread().getId(), writer);
        tl.set(writer);
    }

    public static void write(byte[] bytes) {
        tl.get().write(bytes);
    }
}