package org.embulk.output.bigquery_java;

import java.util.HashMap;

import org.embulk.output.bigquery_java.config.PluginTask;

// This class should be called in TransactionalPageOutput.add
// because thread id is used to determine filename.
// FIXME
// ThreadLocal should remove before exit due to the memory leak however
// Embulk is always up and down therefore we might ignore memory. Holding object in
// local thread belongs singleton as well.
public class BigqueryThreadLocalFileWriter {
    private static ThreadLocal<BigqueryFileWriter> tl = ThreadLocal.withInitial(BigqueryFileWriter::new);
    private static final HashMap<Long, BigqueryFileWriter> writers = BigqueryUtil.getFileWriters();

    public static void setFileWriter(PluginTask task){
        BigqueryFileWriter writer = tl.get();
        long key = Thread.currentThread().getId();
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