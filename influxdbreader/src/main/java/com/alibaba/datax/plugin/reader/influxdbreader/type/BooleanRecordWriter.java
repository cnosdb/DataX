package com.alibaba.datax.plugin.reader.influxdbreader.type;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Record;
import org.apache.arrow.vector.holders.NullableBitHolder;

public class BooleanRecordWriter extends AbstractRecordWriter {
    NullableBitHolder holder;

    public BooleanRecordWriter() {
        super();
        this.holder = new NullableBitHolder();
    }

    @Override
    public void addColumnToRecord(Record record) {
        reader.read(holder);
        record.addColumn(new BoolColumn(holder.value == 1));
    }

    @Override
    public void addNullColumnToRecord(Record record) {
        record.addColumn(new BoolColumn((Boolean) null));
    }
}
