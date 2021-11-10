package org.embulk.output.bigquery_java.config;

import org.embulk.config.ConfigException;

import java.util.Arrays;

public class BigqueryConfigValidator {
    public static void validate(PluginTask task) {
        validateMode(task);
        validateModeAndAutoCreteTable(task);
        validateTimePartitioning(task);
        validateProject(task);
    }

    public static void validateMode(PluginTask task) throws ConfigException {
        // TODO: append_direct delete_in_advance replace_backup
        String[] modes = {"replace", "append", "delete_in_advance", "append_direct"};
        if (!Arrays.asList(modes).contains(task.getMode().toLowerCase())) {
            throw new ConfigException("replace, append, delete_in_advance and append_direct are supported. Stay tuned!");
        }
    }

    public static void validateModeAndAutoCreteTable(PluginTask task) throws ConfigException {
        // TODO: modes are append replace delete_in_advance replace_backup and !task['auto_create_table']
        String[] modes = {"replace", "append", "delete_in_advance"};
        if (Arrays.asList(modes).contains(task.getMode().toLowerCase()) && !task.getAutoCreateTable()) {
            throw new ConfigException("replace, append and delete_in_advance are supported. Stay tuned!");
        }
    }

    public static void validateTimePartitioning(PluginTask task) throws ConfigException {
        if (task.getTimePartitioning().isPresent()) {
            if (!task.getTimePartitioning().get().getType().toUpperCase().equals("DAY")) {
                throw new ConfigException(String.format("`time_partitioning.type: %s` requires `time_partitioning.type: DAY`", task.getMode()));
            }
        }
    }

    public static void validateProject(PluginTask task) throws ConfigException {
        if (!task.getProject().isPresent()){
            throw new ConfigException("project is empty");
        }
        String project = task.getProject().get();
        if (project.isEmpty()){
            throw new ConfigException("project is empty string");
        }
    }
}
