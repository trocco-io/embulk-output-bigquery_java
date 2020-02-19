package org.embulk.output.bigquery_java;

import org.embulk.output.bigquery_java.BigqueryFileWriter;
import org.embulk.output.bigquery_java.config.PluginTask;

public class BigqueryThreadLocalFileWriter {
    private static ThreadLocal<BigqueryFileWriter> tl = new ThreadLocal<BigqueryFileWriter>(){
        @Override
        protected BigqueryFileWriter initialValue(){
            return new BigqueryFileWriter();
        }
    };

    public static void setFileWriter(PluginTask task){
        BigqueryFileWriter writer = tl.get();
        writer.setTask(task);
        writer.setCompression(task.getCompression());
    }

    public static BigqueryFileWriter getFileWriter(){
        return tl.get();
    }

    public static void write(byte[] bytes){
        tl.get().write(bytes);
    }
}