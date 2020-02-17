package org.embulk.output.bigquery_java;

import org.msgpack.core.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class BigqueryConfigBuilder {
    private PluginTask task;
    private String uniqueName;

    public BigqueryConfigBuilder(PluginTask task){
        this.uniqueName = UUID.randomUUID().toString().replace("-", "_");
        this.task = task;
    }

    public PluginTask build(){
        setPathPrefix();
        setFileExt();
        setTempTable();
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
    @VisibleForTesting
    protected void setTempTable(){
        // TODO: support replace_backup, append
        String[] modeForTempTable = {"replace"};
        if (Arrays.asList(modeForTempTable).contains(this.task.getMode())){
            this.task.getTempTable().or(String.format("LOAD_TEMP_%s_%s",this.uniqueName, this.task.getTable()));
        }else{
            task.setTempTable(null);
        }
    }
}
