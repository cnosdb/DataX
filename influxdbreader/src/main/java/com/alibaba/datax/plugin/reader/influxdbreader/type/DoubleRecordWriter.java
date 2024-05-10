package com.alibaba.datax.plugin.reader.influxdbreader.type;

import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.Record;
import org.apache.arrow.vector.holders.NullableFloat8Holder;

public class DoubleRecordWriter extends AbstractRecordWriter {
    NullableFloat8Holder holder;

    public DoubleRecordWriter() {
        super();
        this.holder = new NullableFloat8Holder();
    }

    @Override
    public void addColumnToRecord(Record record) {
        reader.read(holder);
        record.addColumn(new DoubleColumn(holder.value));
    }

    @Override
    public void addNullColumnToRecord(Record record) {
        record.addColumn(new DoubleColumn((Double) null));
    }
}
