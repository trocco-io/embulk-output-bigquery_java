package org.embulk.output.bigquery_java.visitor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.output.bigquery_java.config.BigqueryColumnOption;
import org.embulk.output.bigquery_java.BigqueryUtil;
import org.embulk.output.bigquery_java.BigqueryValueConverter;
import org.embulk.output.bigquery_java.config.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class JacksonJsonColumnVisitor implements BigqueryColumnVisitor {
    private PluginTask task;
    final PageReader reader;
    private List<BigqueryColumnOption> columnOptions;
    private final ObjectNode node;

    public JacksonJsonColumnVisitor(PluginTask task, PageReader reader, List<BigqueryColumnOption> columnOptions) {
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
        }else{
            node.put(column.getName(), reader.getBoolean(column));
        }
    }

    @Override
    public void longColumn(Column column) {
        if (reader.isNull(column)){
            node.putNull(column.getName());
        }else{
            node.put(column.getName(), reader.getLong(column));
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (reader.isNull(column)){
            node.putNull(column.getName());
        }else {
            node.put(column.getName(), reader.getDouble(column));
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (reader.isNull(column)){
            node.putNull(column.getName());
        }else {
            Optional<BigqueryColumnOption> columnOption = BigqueryUtil.findColumnOption(column.getName(), this.columnOptions);
            if (columnOption.isPresent() && columnOption.get().getType().isPresent()) {
                BigqueryValueConverter.convertAndSet(this.node, column.getName(),
                        reader.getString(column), columnOption.get(), this.task);
            } else {
                node.put(column.getName(), reader.getString(column));
            }
        }
    }

    @Override
    public void timestampColumn(Column column) {
        if (reader.isNull(column)){
            node.putNull(column.getName());
        }else {
            node.put(column.getName(), reader.getString(column));
        }

        // TODO:
        // TimestampFormatter formatter = timestampFormatters[column.getIndex()];
        // Value value = ValueFactory.newString(formatter.format(reader.getTimestamp(column)));
    }

    @Override
    public void jsonColumn(Column column) {
        if (reader.isNull(column)){
            node.putNull(column.getName());
        }else {
            node.put(column.getName(), reader.getString(column));
        }
    }
}
