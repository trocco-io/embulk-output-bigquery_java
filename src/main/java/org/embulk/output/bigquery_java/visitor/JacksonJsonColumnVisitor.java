package org.embulk.output.bigquery_java.visitor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.output.bigquery_java.BigqueryColumnOption;
import org.embulk.output.bigquery_java.BigqueryUtil;
import org.embulk.output.bigquery_java.BigqueryValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class JacksonJsonColumnVisitor implements BigqueryColumnVisitor {
    final PageReader reader;
    private List<BigqueryColumnOption> columnOptions;
    private final ObjectNode node;

    public JacksonJsonColumnVisitor(PageReader reader, List<BigqueryColumnOption> columnOptions) {
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
        node.put(column.getName(), reader.getBoolean(column));
    }

    @Override
    public void longColumn(Column column) {
        node.put(column.getName(), reader.getLong(column));
    }

    @Override
    public void doubleColumn(Column column) {
        node.put(column.getName(), reader.getDouble(column));
    }

    @Override
    public void stringColumn(Column column) {
        Optional<BigqueryColumnOption> columnOption = BigqueryUtil.findColumnOption(column.getName(), this.columnOptions);
        if (columnOption.isPresent()){
            BigqueryValueConverter.convertAndSet(this.node, column.getName(),
                    reader.getString(column), columnOption.get());
        }else{
            node.put(column.getName(), reader.getString(column));
        }
    }

    @Override
    public void timestampColumn(Column column) {
        node.put(column.getName(), reader.getString(column));

        // TODO:
        // TimestampFormatter formatter = timestampFormatters[column.getIndex()];
        // Value value = ValueFactory.newString(formatter.format(reader.getTimestamp(column)));
    }

    @Override
    public void jsonColumn(Column column) {
        node.put(column.getName(), reader.getString(column));
    }
}
