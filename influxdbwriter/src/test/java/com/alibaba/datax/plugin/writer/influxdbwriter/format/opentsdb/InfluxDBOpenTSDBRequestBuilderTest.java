package com.alibaba.datax.plugin.writer.influxdbwriter.format.opentsdb;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.influxdbwriter.InfluxDBWriter;
import com.alibaba.datax.plugin.writer.influxdbwriter.InfluxWriterConfigFieldExtra;
import com.alibaba.datax.plugin.writer.influxdbwriter.format.IInfluxDBRequestBuilder;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class InfluxDBOpenTSDBRequestBuilderTest extends TestCase {
    public void testAppendRecord() {
        int precisionToMultiplier = InfluxDBWriter.precisionToMultiplier("ms");

        IInfluxDBRequestBuilder builder = new InfluxDBOpenTSDBRequestBuilder(1024, 2, precisionToMultiplier);

        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a1");
            InfluxDBOpenTSDBPoint p1 = new InfluxDBOpenTSDBPoint(10001, "test", tags, 1L);
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn(p1.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r1);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a2");
            InfluxDBOpenTSDBPoint p2 = new InfluxDBOpenTSDBPoint(10002, "test", tags, 2L);
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn(p2.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1 value=1 10001000000\n" +
                    "test,ta=a2 value=2 10002000000\n", write_batch.get().toString());
        }
        builder.clear();
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a3");
            InfluxDBOpenTSDBPoint p3 = new InfluxDBOpenTSDBPoint(10003, "test", tags, 3.0);
            Record r3 = new DefaultRecord();
            r3.addColumn(new StringColumn(p3.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r3);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a4");
            InfluxDBOpenTSDBPoint p4 = new InfluxDBOpenTSDBPoint(10004, "test", tags, 4.0);
            Record r4 = new DefaultRecord();
            r4.addColumn(new StringColumn(p4.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r4);
            assert write_batch.isPresent();
            assertEquals("test,ta=a3 value=3.0 10003000000\n" +
                    "test,ta=a4 value=4.0 10004000000\n", write_batch.get().toString());
        }
    }

    public void testAppendRecordWithExtra() {
        int precisionToMultiplier = InfluxDBWriter.precisionToMultiplier("ms");
        Map<String, String> tagsExtra = new HashMap<String, String>() {{
            put("t1", "t1_1");
            put("t2", "t2_1");
        }};
        Map<String, String> tagsExtraMetric2 = new HashMap<String, String>() {{
            put("ta", "ta_1");
            put("tb", "tb_1");
        }};
        Map<String, InfluxWriterConfigFieldExtra> fieldsExtra = new HashMap<String, InfluxWriterConfigFieldExtra>() {{
            put("metric_1", new InfluxWriterConfigFieldExtra("table_1", "f1", null));
            put("metric_2", new InfluxWriterConfigFieldExtra("table_2", "f2", tagsExtraMetric2));
            put("metric_3", new InfluxWriterConfigFieldExtra("table_2", "f3", tagsExtraMetric2));
        }};
        InfluxDBWriter.mergeFieldsExtraTags(fieldsExtra, tagsExtra);

        IInfluxDBRequestBuilder builder = new InfluxDBOpenTSDBRequestBuilder(1024, 2, precisionToMultiplier,
                tagsExtra, fieldsExtra);

        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a1");
            InfluxDBOpenTSDBPoint p1 = new InfluxDBOpenTSDBPoint(10001, "metric_1", tags, 1L);
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn(p1.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r1);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("tb", "b1");
            InfluxDBOpenTSDBPoint p2 = new InfluxDBOpenTSDBPoint(10002, "metric_2", tags, 2L);
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn(p2.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r2);
            assert write_batch.isPresent();
            assertEquals("table_1,ta=a1,t1=t1_1,t2=t2_1 f1=1 10001000000\n" +
                    "table_2,tb=b1,ta=ta_1 f2=2 10002000000\n", write_batch.get().toString());
        }
        builder.clear();

        // Test set the default extra tags.
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a2");
            InfluxDBOpenTSDBPoint p3 = new InfluxDBOpenTSDBPoint(10003, "metric_2", tags, 3.0);
            Record r3 = new DefaultRecord();
            r3.addColumn(new StringColumn(p3.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r3);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("tb", "b2");
            InfluxDBOpenTSDBPoint p4 = new InfluxDBOpenTSDBPoint(10004, "metric_2", tags, 4.0);
            Record r4 = new DefaultRecord();
            r4.addColumn(new StringColumn(p4.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r4);
            assert write_batch.isPresent();
            assertEquals("table_2,ta=a2,tb=tb_1 f2=3.0 10003000000\n" +
                    "table_2,tb=b2,ta=ta_1 f2=4.0 10004000000\n", write_batch.get().toString());
        }
        builder.clear();

        // Test metrics to be put to the same table.
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a3");
            InfluxDBOpenTSDBPoint p3 = new InfluxDBOpenTSDBPoint(10005, "metric_2", tags, 5.0);
            Record r3 = new DefaultRecord();
            r3.addColumn(new StringColumn(p3.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r3);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("tb", "b3");
            InfluxDBOpenTSDBPoint p4 = new InfluxDBOpenTSDBPoint(10006, "metric_3", tags, 6.0);
            Record r4 = new DefaultRecord();
            r4.addColumn(new StringColumn(p4.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r4);
            assert write_batch.isPresent();
            assertEquals("table_2,ta=a3,tb=tb_1 f2=5.0 10005000000\n" +
                    "table_2,tb=b3,ta=ta_1 f3=6.0 10006000000\n", write_batch.get().toString());
        }
    }

    public void testPrecisionMultiplier() {
        int precisionToMultiplier = InfluxDBWriter.precisionToMultiplier("s");
        IInfluxDBRequestBuilder builder = new InfluxDBOpenTSDBRequestBuilder(0, 0, precisionToMultiplier);
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a1");
            InfluxDBOpenTSDBPoint p1 = new InfluxDBOpenTSDBPoint(10001, "test", tags, 1L);
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn(p1.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r1);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1 value=1 10001000000000\n", write_batch.get().toString());
        }
        builder.clear();
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a2");
            InfluxDBOpenTSDBPoint p2 = new InfluxDBOpenTSDBPoint(16876L, "test", tags, 2L);
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn(p2.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a2 value=2 16876000000000\n", write_batch.get().toString());
        }
    }
}
