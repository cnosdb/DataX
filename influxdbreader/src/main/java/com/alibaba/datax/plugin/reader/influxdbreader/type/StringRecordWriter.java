package com.alibaba.datax.plugin.reader.influxdbreader.type;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;

public class StringRecordWriter extends AbstractRecordWriter {
    public StringRecordWriter() {
        super();
    }

    @Override
    public void addColumnToRecord(Record record) {
        record.addColumn(new StringColumn(reader.readText().toString()));
    }

    @Override
    public void addNullColumnToRecord(Record record) {
        record.addColumn(new StringColumn(null));
    }
}
