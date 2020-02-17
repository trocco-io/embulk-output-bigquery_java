package org.embulk.output.bigquery_java.visitor;


import org.embulk.output.bigquery_java.BigqueryColumnOption;
import org.embulk.output.bigquery_java.BigqueryUtil;
import org.embulk.output.bigquery_java.BigqueryValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import com.google.common.base.Optional;;


// this file will be removed if JacksonJsonColumnVisitor is faster
// just leave here for benchmarking
public class JsonColumnVisitor implements BigqueryColumnVisitor {
    final PageReader reader;
    private List<BigqueryColumnOption> columnOptions;
    private final ValueFactory.MapBuilder builder;

    public JsonColumnVisitor(PageReader reader, List<BigqueryColumnOption> columnOptions) {
        this.reader = reader;
        this.columnOptions = columnOptions;
        this.builder = new ValueFactory.MapBuilder();
    }

    public byte[] getByteArray() {
        Value value = builder.build();
        String json = value.toJson() + "\n";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void booleanColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = ValueFactory.newBoolean(reader.getBoolean(column));
        builder.put(columnName, value);
    }

    @Override
    public void longColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = ValueFactory.newInteger(reader.getLong(column));
        builder.put(columnName, value);
    }

    @Override
    public void doubleColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = ValueFactory.newFloat(reader.getDouble(column));
        builder.put(columnName, value);
    }

    @Override
    public void stringColumn(Column column) {
        Value value;
        Value columnName = ValueFactory.newString(column.getName());
        // Optional<BigqueryColumnOption> columnOption = BigqueryUtil.findColumnOption(column.getName(), this.columnOptions);

        // if (columnOption.isPresent()){
        //     value = BigqueryValueConverter.convert(reader.getString(column), columnOption.get());
        // }else{
        //     value = ValueFactory.newString(reader.getString(column));
        // }
        value = ValueFactory.newString(reader.getString(column));

        builder.put(columnName, value);
    }

    @Override
    public void timestampColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = ValueFactory.newString("");

        // TODO:
        // TimestampFormatter formatter = timestampFormatters[column.getIndex()];
        // Value value = ValueFactory.newString(formatter.format(reader.getTimestamp(column)));
        builder.put(columnName, value);
    }

    @Override
    public void jsonColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = reader.getJson(column);
        builder.put(columnName, value);
    }
}