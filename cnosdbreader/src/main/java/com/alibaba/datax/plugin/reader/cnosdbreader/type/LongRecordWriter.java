package com.alibaba.datax.plugin.reader.cnosdbreader.type;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableIntHolder;

public class LongRecordWriter extends AbstractRecordWriter {
    NullableIntHolder holder;

    public LongRecordWriter() {
        super();
        this.holder = new NullableIntHolder();
    }

    @Override
    public void addColumnToRecord(Record record) {
        reader.read(holder);
        record.addColumn(new LongColumn((long) holder.value));
    }

    @Override
    public void addNullColumnToRecord(Record record) {
        record.addColumn(new LongColumn((Long) null));
    }
}
