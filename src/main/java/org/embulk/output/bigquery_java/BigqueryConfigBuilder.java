package org.embulk.output.bigquery_java;

import org.msgpack.core.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;

public class BigqueryConfigBuilder {
    private PluginTask task;

    public BigqueryConfigBuilder(PluginTask task){
        this.task = task;
    }

    public PluginTask build(){
        setPathPrefix();

        return this.task;
    }

    @VisibleForTesting
    protected void setPathPrefix(){
        if (!this.task.getPathPrefix().isPresent()){
            try {
                File tmpFile = File.createTempFile("embulk_output_bigquery_java", "");
                this.task.setPathPrefix(tmpFile.getPath());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @VisibleForTesting
    protected void setFileExt(){
        if (!this.task.getFileExt().isPresent()){
            if (this.task.getSourceFormat().equals("CSV")){
                this.task.setFileExt(".csv");
            }else{
                this.task.setFileExt(".jsonl");
            }
        }

        if (this.task.getCompression().equals("GZIP")){
            this.task.setFileExt(this.task.getFileExt() + ".gz");
        }
    }

    // TODO: set columnOption.Type as Enum

}
