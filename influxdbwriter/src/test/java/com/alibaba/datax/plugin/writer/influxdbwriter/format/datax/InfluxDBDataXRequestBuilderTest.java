package com.alibaba.datax.plugin.writer.influxdbwriter.format.datax;

import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.influxdbwriter.InfluxDBWriter;
import com.alibaba.datax.plugin.writer.influxdbwriter.format.IInfluxDBRequestBuilder;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class InfluxDBDataXRequestBuilderTest extends TestCase {

    public void testAppendRecord() {
        HashMap<Integer, String> tagIndexes = new HashMap<>();
        tagIndexes.put(0, "ta");
        HashMap<Integer, String> fieldIndexes = new HashMap<>();
        fieldIndexes.put(1, "fa");
        fieldIndexes.put(3, "fb");
        int timeIndex = 2;
        int precisionToMultiplier = InfluxDBWriter.precisionToMultiplier("ms");
        IInfluxDBRequestBuilder builder = new InfluxDBDataXRequestBuilder(1024, 2, precisionToMultiplier,
                "test", tagIndexes, fieldIndexes, timeIndex);
        {
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn("a1"));
            r1.addColumn(new DoubleColumn(1.0));
            r1.addColumn(new LongColumn(10001L));
            r1.addColumn(new StringColumn("fb1"));
            Optional<CharSequence> write_batch = builder.append(r1);
            assert !write_batch.isPresent();
        }
        {
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn("a2"));
            r2.addColumn(new DoubleColumn(2F));
            r2.addColumn(new LongColumn(10002L));
            r2.addColumn(new StringColumn("fb2"));
            Optional<CharSequence> write_batch = builder.append(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1 fa=1.0,fb=\"fb1\" 10001000000\n" +
                    "test,ta=a2 fa=2.0,fb=\"fb2\" 10002000000\n", write_batch.get().toString());
        }
        builder.clear();
        {
            Record r3 = new DefaultRecord();
            r3.addColumn(new StringColumn("a3"));
            r3.addColumn(new DoubleColumn(3.0));
            r3.addColumn(new LongColumn(10003L));
            r3.addColumn(new StringColumn("fb3"));
            Optional<CharSequence> write_batch = builder.append(r3);
            assert !write_batch.isPresent();
        }
        {
            Record r4 = new DefaultRecord();
            r4.addColumn(new StringColumn("a4"));
            r4.addColumn(new DoubleColumn(4F));
            r4.addColumn(new LongColumn(10004L));
            r4.addColumn(new StringColumn("fb4"));
            Optional<CharSequence> write_batch = builder.append(r4);
            assert write_batch.isPresent();
            assertEquals("test,ta=a3 fa=3.0,fb=\"fb3\" 10003000000\n" +
                    "test,ta=a4 fa=4.0,fb=\"fb4\" 10004000000\n", write_batch.get().toString());
        }
    }

    public void testAppendRecordWithTagsExtra() {
        HashMap<Integer, String> tagIndexes = new HashMap<>();
        tagIndexes.put(0, "ta");
        HashMap<Integer, String> fieldIndexes = new HashMap<>();
        fieldIndexes.put(1, "fa");
        fieldIndexes.put(3, "fb");
        int timeIndex = 2;
        int precisionToMultiplier = InfluxDBWriter.precisionToMultiplier("ms");
        Map<String, String> tagsExtra = new HashMap<String, String>() {{
            put("t1", "t1_1");
            put("t2", "t2_1");
        }};
        IInfluxDBRequestBuilder builder = new InfluxDBDataXRequestBuilder(1024, 2, precisionToMultiplier,
                "test", tagIndexes, fieldIndexes, timeIndex, tagsExtra);
        {
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn("a1"));
            r1.addColumn(new DoubleColumn(1.0));
            r1.addColumn(new LongColumn(10001L));
            r1.addColumn(new StringColumn("fb1"));
            Optional<CharSequence> write_batch = builder.append(r1);
            assert !write_batch.isPresent();
        }
        {
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn("a2"));
            r2.addColumn(new DoubleColumn(2F));
            r2.addColumn(new LongColumn(10002L));
            r2.addColumn(new StringColumn("fb2"));
            Optional<CharSequence> write_batch = builder.append(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1,t1=t1_1,t2=t2_1 fa=1.0,fb=\"fb1\" 10001000000\n" +
                    "test,ta=a2,t1=t1_1,t2=t2_1 fa=2.0,fb=\"fb2\" 10002000000\n", write_batch.get().toString());
        }
        builder.clear();
        {
            Record r3 = new DefaultRecord();
            r3.addColumn(new StringColumn("a3"));
            r3.addColumn(new DoubleColumn(3.0));
            r3.addColumn(new LongColumn(10003L));
            r3.addColumn(new StringColumn("fb3"));
            Optional<CharSequence> write_batch = builder.append(r3);
            assert !write_batch.isPresent();
        }
        {
            Record r4 = new DefaultRecord();
            r4.addColumn(new StringColumn("a4"));
            r4.addColumn(new DoubleColumn(4F));
            r4.addColumn(new LongColumn(10004L));
            r4.addColumn(new StringColumn("fb4"));
            Optional<CharSequence> write_batch = builder.append(r4);
            assert write_batch.isPresent();
            assertEquals("test,ta=a3,t1=t1_1,t2=t2_1 fa=3.0,fb=\"fb3\" 10003000000\n" +
                    "test,ta=a4,t1=t1_1,t2=t2_1 fa=4.0,fb=\"fb4\" 10004000000\n", write_batch.get().toString());
        }
    }

    public void testPrecisionMultiplier() {
        HashMap<Integer, String> tagIndexes = new HashMap<>();
        tagIndexes.put(0, "ta");
        HashMap<Integer, String> fieldIndexes = new HashMap<>();
        fieldIndexes.put(1, "fa");
        fieldIndexes.put(3, "fb");
        int timeIndex = 2;
        int precisionMultiplier = InfluxDBWriter.precisionToMultiplier("ns");
        IInfluxDBRequestBuilder builder = new InfluxDBDataXRequestBuilder(0, 0, precisionMultiplier, "test", tagIndexes, fieldIndexes, timeIndex);
        {
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn("a1"));
            r1.addColumn(new DoubleColumn(1.0));
            r1.addColumn(new LongColumn(10001L));
            r1.addColumn(new StringColumn("fb1"));
            Optional<CharSequence> write_batch = builder.append(r1);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1 fa=1.0,fb=\"fb1\" 10001\n", write_batch.get().toString());
        }
        builder.clear();
        {
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn("a2"));
            r2.addColumn(new DoubleColumn(2.0));
            r2.addColumn(new LongColumn(1687622400000L));
            r2.addColumn(new StringColumn("fb2"));
            Optional<CharSequence> write_batch = builder.append(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a2 fa=2.0,fb=\"fb2\" 1687622400000\n", write_batch.get().toString());
        }
    }
}