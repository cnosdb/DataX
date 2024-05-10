package com.alibaba.datax.plugin.reader.cnosdbreader.type;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import org.apache.arrow.vector.complex.reader.FieldReader;

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
