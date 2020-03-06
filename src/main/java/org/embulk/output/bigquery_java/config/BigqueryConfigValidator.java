package org.embulk.output.bigquery_java.config;

import org.embulk.config.ConfigException;

import java.util.Arrays;

public class BigqueryConfigValidator {
    public static void validate(PluginTask task){
       validateMode(task);
    }

    public static void validateMode(PluginTask task) throws ConfigException{
        String[] modes = {"replace", "append"};
        if (!Arrays.asList(modes).contains(task.getMode())) {
            throw new ConfigException("replace and replace are supported. Stay tune!");
        }
    }
}
