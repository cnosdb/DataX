package com.alibaba.datax.plugin.writer.influxdbwriter.format.opentsdb;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.influxdbwriter.InfluxDBWriteBatchBuilder;
import com.alibaba.datax.plugin.writer.influxdbwriter.InfluxWriterConfigFieldExtra;
import com.alibaba.datax.plugin.writer.influxdbwriter.InfluxDBWriterErrorCode;
import com.alibaba.datax.plugin.writer.influxdbwriter.InfluxDBWriterException;
import com.alibaba.datax.plugin.writer.influxdbwriter.format.IInfluxDBRequestBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class InfluxDBOpenTSDBRequestBuilder implements IInfluxDBRequestBuilder {
    private int count;
    private final int batchSize;
    private final InfluxDBWriteBatchBuilder buffer;
    private final Consumer<Long> timeAppender;
    /**
     * Config tagsExtra, will never be null.
     */
    private final Map<String, String> tagsExtra;
    /**
     * Config fieldsExtra.
     * <p>
     * If config fieldsExtra[*].tagsExtra is null, it will be the root tagsExtra.
     * So you only need to check the fieldsExtra[metric], or tagsExtra if fieldsExtra[metric] is null in one time.
     */
    private final Map<String, InfluxWriterConfigFieldExtra> fieldsExtra;

    public InfluxDBOpenTSDBRequestBuilder(int capacity, int batchSize, int precisionMultiplier) {
        this(capacity, batchSize, precisionMultiplier, null, null);
    }

    public InfluxDBOpenTSDBRequestBuilder(
            int capacity, int batchSize, int precisionMultiplier,
            Map<String, String> tagsExtra, Map<String, InfluxWriterConfigFieldExtra> fieldsExtra
    ) {
        this.count = 0;
        this.batchSize = batchSize;
        this.buffer = new InfluxDBWriteBatchBuilder(capacity);

        if (precisionMultiplier == 1) {
            this.timeAppender = this.buffer::appendTime;
        } else {
            // For string 1000000, get 000000
            String suffix = String.valueOf(precisionMultiplier).substring(1);
            this.timeAppender = i -> {
                this.buffer.appendTime(i);
                this.buffer.appendTime(suffix);
            };
        }

        if (tagsExtra == null) {
            this.tagsExtra = new HashMap<>(0);
        } else {
            this.tagsExtra = tagsExtra;
        }
        if (fieldsExtra == null) {
            this.fieldsExtra = new HashMap<>(0);
        } else {
            this.fieldsExtra = fieldsExtra;
        }
    }

    @Override
    public Optional<CharSequence> append(Record record) throws InfluxDBWriterException {
        String line = record.getColumn(0).asString();
        InfluxDBOpenTSDBPoint pt;
        try {
            pt = InfluxDBOpenTSDBPoint.fromJSON(line);
        } catch (Exception e) {
            String message = String.format("解码 JSON 格式的 OpenTSDB Point 失败: %s", e.getMessage());
            throw new InfluxDBWriterException(InfluxDBWriterErrorCode.ParseJSONOpenTSDB, message);
        }

        InfluxWriterConfigFieldExtra fieldExtra = this.fieldsExtra.get(pt.getMetric());
        if (fieldExtra == null) {
            this.buffer.startWriteRecord(pt.getMetric());
            this.bufferAppendTags(pt.getTags(), null);
            // Using "value" as field name, OpenTSDB value is long or double
            this.buffer.appendDoubleField("value", pt.getValue().toString());
        } else {
            // Using fieldsExtra[*].table as table
            this.buffer.startWriteRecord(fieldExtra.getTable());
            this.bufferAppendTags(pt.getTags(), fieldExtra.getTagsExtra());
            // Using fieldsExtra[*].field as field name, OpenTSDB value is long or double
            this.buffer.appendDoubleField(fieldExtra.getField(), pt.getValue().toString());
        }

        this.timeAppender.accept(pt.getTimestamp());
        this.buffer.endWriteRecord();

        this.count += 1;
        if (this.buffer.isFull() || this.count >= this.batchSize) {
            this.count = 0;
            return Optional.of(this.buffer.getBuffer());
        } else {
            return Optional.empty();
        }
    }

    private void bufferAppendTags(Map<String, String> tagsMap, Map<String, String> fieldTagsExtra) {
        if (tagsMap != null) {
            for (Map.Entry<String, String> e : tagsMap.entrySet()) {
                this.buffer.appendTag(e.getKey(), e.getValue());
            }
            if (fieldTagsExtra != null) {
                for (Map.Entry<String, String> e : fieldTagsExtra.entrySet()) {
                    if (!tagsMap.containsKey(e.getKey())) {
                        this.buffer.appendTag(e.getKey(), e.getValue());
                    }
                }
            } else if (!this.tagsExtra.isEmpty()) {
                for (Map.Entry<String, String> e : this.tagsExtra.entrySet()) {
                    if (!tagsMap.containsKey(e.getKey())) {
                        this.buffer.appendTag(e.getKey(), e.getValue());
                    }
                }
            }
        } else {
            if (fieldTagsExtra != null) {
                for (Map.Entry<String, String> e : fieldTagsExtra.entrySet()) {
                    this.buffer.appendTag(e.getKey(), e.getValue());
                }
            } else if (!this.tagsExtra.isEmpty()) {
                for (Map.Entry<String, String> e : this.tagsExtra.entrySet()) {
                    this.buffer.appendTag(e.getKey(), e.getValue());
                }
            }
        }
    }

    @Override
    public CharSequence get() {
        return this.buffer.getBuffer();
    }

    @Override
    public void clear() {
        this.buffer.clearBuffer();
    }

    @Override
    public int length() {
        return this.buffer.length();
    }
}