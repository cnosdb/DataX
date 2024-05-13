package com.alibaba.datax.plugin.reader.cnosdbreader.type;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import org.apache.arrow.vector.holders.NullableTimeStampMicroHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecHolder;

public class TimestampRecordWriter {
    public static class Second extends AbstractRecordWriter {
        NullableTimeStampSecHolder holder;

        public Second() {
            super();
            this.holder = new NullableTimeStampSecHolder();
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

    public static class MilliSecond extends AbstractRecordWriter {
        NullableTimeStampMilliHolder holder;

        public MilliSecond() {
            super();
            this.holder = new NullableTimeStampMilliHolder();
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

    public static class MicroSecond extends AbstractRecordWriter {
        NullableTimeStampMicroHolder holder;

        public MicroSecond() {
            super();
            this.holder = new NullableTimeStampMicroHolder();
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

    public static class NanoSecond extends AbstractRecordWriter {
        NullableTimeStampNanoHolder holder;

        public NanoSecond() {
            super();
            this.holder = new NullableTimeStampNanoHolder();
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

}
