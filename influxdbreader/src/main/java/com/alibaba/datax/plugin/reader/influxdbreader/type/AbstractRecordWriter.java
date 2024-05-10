package com.alibaba.datax.plugin.reader.influxdbreader.type;

import com.alibaba.datax.common.element.Record;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;

public abstract class AbstractRecordWriter {
    FieldReader reader;
    int valueCount;

    int index;

    public AbstractRecordWriter() {
        this.reader = null;
        this.valueCount = 0;
        this.index = 0;
    }

    public static AbstractRecordWriter build(ArrowType arrowType) {
        if (arrowType instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp type = (ArrowType.Timestamp) arrowType;
            switch (type.getUnit()) {
                case SECOND:
                    return new TimestampRecordWriter.Second();
                case MILLISECOND:
                    return new TimestampRecordWriter.MilliSecond();
                case MICROSECOND:
                    return new TimestampRecordWriter.MicroSecond();
                case NANOSECOND:
                    return new TimestampRecordWriter.NanoSecond();
            }
        } else if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
            return new StringRecordWriter();
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            return new DoubleRecordWriter();
        } else if (arrowType instanceof ArrowType.Int) {
            return new LongRecordWriter();
        } else if (arrowType instanceof ArrowType.Bool) {
            return new BooleanRecordWriter();
        }

        throw new IllegalArgumentException("Unsupported arrow type: " + arrowType);
    }

    public void reset(FieldReader reader, int valueCount) {
        this.reader = reader;
        this.valueCount = valueCount;
        this.index = 0;
    }

    public boolean isFinished() {
        return index >= valueCount;
    }

    abstract void addColumnToRecord(Record record);

    abstract void addNullColumnToRecord(Record record);

    /**
     * Read next value from FieldReader and write to record.
     *
     * @param record Record
     * @return true if FieldReader is not finished and a new Column was written to record,
     * otherwise it means FieldReader is finished and did nothing to record.
     */
    public boolean writeNextRecord(Record record) {
        if (this.isFinished()) {
            return false;
        }
        reader.setPosition(index);
        if (reader.isSet()) {
            addColumnToRecord(record);
        } else {
            addNullColumnToRecord(record);
        }
        index++;
        return true;
    }
}
