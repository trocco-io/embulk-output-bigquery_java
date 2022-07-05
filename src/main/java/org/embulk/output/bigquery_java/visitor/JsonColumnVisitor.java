package org.embulk.output.bigquery_java.visitor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.BigqueryUtil;
import org.embulk.output.bigquery_java.BigqueryValueConverter;
import org.embulk.output.bigquery_java.config.BigqueryColumnOptionType;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonColumnVisitor implements BigqueryColumnVisitor {
    final PageReader reader;
    private final ObjectNode node;
    private PluginTask task;
    private List<BigqueryColumnOption> columnOptions;

    public JsonColumnVisitor(PluginTask task, PageReader reader, List<BigqueryColumnOption> columnOptions) {
        this.task = task;
        this.reader = reader;
        this.columnOptions = columnOptions;
        this.node = BigqueryUtil.getObjectMapper().createObjectNode();
    }

    public byte[] getByteArray() {
        String json = node.toString() + "\n";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void booleanColumn(Column column) {
        if (reader.isNull(column)) {
            node.putNull(column.getName());
        } else {
            Optional<BigqueryColumnOption> columnOption = BigqueryUtil.findColumnOption(column.getName(), this.columnOptions);
            BigqueryColumnOptionType bigqueryColumnOptionType;
            if (columnOption.isPresent() && columnOption.get().getType().isPresent()) {
                bigqueryColumnOptionType = BigqueryColumnOptionType.valueOf(columnOption.get().getType().get());
            }else{
                bigqueryColumnOptionType = BigqueryColumnOptionType.BOOLEAN;
            }
            BigqueryValueConverter.convertAndSet(
                    this.node,
                    column.getName(),
                    reader.getBoolean(column),
                    bigqueryColumnOptionType);
        }
    }

    @Override
    public void longColumn(Column column) {
        if (reader.isNull(column)) {
            node.putNull(column.getName());
        } else {
            Optional<BigqueryColumnOption> columnOption = BigqueryUtil.findColumnOption(column.getName(), this.columnOptions);
            BigqueryColumnOptionType bigqueryColumnOptionType;
            if (columnOption.isPresent() && columnOption.get().getType().isPresent()) {
                bigqueryColumnOptionType = BigqueryColumnOptionType.valueOf(columnOption.get().getType().get());
            }else{
                bigqueryColumnOptionType = BigqueryColumnOptionType.INTEGER;
            }
            BigqueryValueConverter.convertAndSet(
                    this.node,
                    column.getName(),
                    reader.getLong(column),
                    bigqueryColumnOptionType);
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (reader.isNull(column)) {
            node.putNull(column.getName());
        } else {
            Optional<BigqueryColumnOption> columnOption = BigqueryUtil.findColumnOption(column.getName(), this.columnOptions);
            BigqueryColumnOptionType bigqueryColumnOptionType;
            if (columnOption.isPresent() && columnOption.get().getType().isPresent()) {
                bigqueryColumnOptionType = BigqueryColumnOptionType.valueOf(columnOption.get().getType().get());
            }else{
                bigqueryColumnOptionType = BigqueryColumnOptionType.FLOAT;
            }
            BigqueryValueConverter.convertAndSet(
                    this.node,
                    column.getName(),
                    reader.getDouble(column),
                    bigqueryColumnOptionType);
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (reader.isNull(column)) {
            node.putNull(column.getName());
        } else {
            Optional<BigqueryColumnOption> columnOption = BigqueryUtil.findColumnOption(column.getName(), this.columnOptions);
            BigqueryColumnOptionType bigqueryColumnOptionType;
            BigqueryColumnOption bigqueryColumnOption = null;
            if (columnOption.isPresent() && columnOption.get().getType().isPresent()) {
                bigqueryColumnOptionType = BigqueryColumnOptionType.valueOf(columnOption.get().getType().get());
                bigqueryColumnOption = columnOption.get();
            }else{
                bigqueryColumnOptionType = BigqueryColumnOptionType.STRING;
            }
            BigqueryValueConverter.convertAndSet(
                    this.node,
                    column.getName(),
                    reader.getString(column),
                    bigqueryColumnOptionType,
                    bigqueryColumnOption);
        }
    }

    @Override
    public void timestampColumn(Column column) {
        if (reader.isNull(column)) {
            node.putNull(column.getName());
        } else {
            Optional<BigqueryColumnOption> columnOption = BigqueryUtil.findColumnOption(column.getName(), this.columnOptions);
            BigqueryColumnOptionType bigqueryColumnOptionType;
            BigqueryColumnOption bigqueryColumnOption = null;
            if (columnOption.isPresent() && columnOption.get().getType().isPresent()) {
                bigqueryColumnOptionType = BigqueryColumnOptionType.valueOf(columnOption.get().getType().get());
                bigqueryColumnOption = columnOption.get();
            }else{
                bigqueryColumnOptionType = BigqueryColumnOptionType.TIMESTAMP;
            }
            BigqueryValueConverter.convertAndSet(
                    this.node,
                    column.getName(),
                    reader.getTimestamp(column),
                    bigqueryColumnOptionType,
                    bigqueryColumnOption,
                    this.task);
        }
    }

    @Override
    public void jsonColumn(Column column) {
        if (reader.isNull(column)) {
            node.putNull(column.getName());
        } else {
            node.put(column.getName(), reader.getJson(column).toJson());
        }
    }
}
