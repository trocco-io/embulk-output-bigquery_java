package org.embulk.output.bigquery_java;

import org.msgpack.core.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.Optional;

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
                System.out.println("=============================");
                System.out.println(tmpFile.getPath());
                this.task.setPathPrefix(Optional.of(tmpFile.getPath()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @VisibleForTesting
    protected void setFileExt(){
        if (!this.task.getFileExt().isPresent()){
            if (this.task.getSourceFormat().equals("CSV")){
                this.task.setFileExt(Optional.of(".csv"));
            }else{
                this.task.setFileExt(Optional.of(".jsonl"));
            }
        }

        if (this.task.getCompression().equals("GZIP")){
            String fileExt = this.task.getFileExt().get() + ".gz";
            this.task.setFileExt(Optional.of(fileExt));
        }
    }
    @VisibleForTesting
    protected void setTempTable(){
        // TODO: support replace_backup, append
        String[] modeForTempTable = {"replace"};
        if (Arrays.asList(modeForTempTable).contains(this.task.getMode())){
            String tempTable = this.task.getTempTable().orElse(String.format("LOAD_TEMP_%s_%s",this.uniqueName, this.task.getTable()));
            this.task.setTempTable(Optional.of(tempTable));
        }else{
            this.task.setTempTable(Optional.of(null));
        }
    }
}
