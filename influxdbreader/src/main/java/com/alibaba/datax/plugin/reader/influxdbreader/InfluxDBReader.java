package com.alibaba.datax.plugin.reader.influxdbreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.influxdbreader.type.AbstractRecordWriter;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class InfluxDBReader extends Reader {
    public final static String CFG_IP = "ip";
    public final static String CFG_PORT = "port";
    public final static String CFG_TIMEOUT_SECONDS = "timeoutSeconds";
    public final static String CFG_TENANT = "tenant";
    public final static String CFG_DATABASE = "database";
    public final static String CFG_USERNAME = "username";
    public final static String CFG_PASSWORD = "password";
    public final static String CFG_SQL = "sql";

    final static String ERR_MISSING_CFG_FIELD = "缺少必填的配置项: '%s'";
    final static String ERR_INVALID_CFG = "配置项不正确: '%s', %s";
    final static String ERR_INVALID_CFG_PRECISION = "配置项不正确: 'precision': %s, 配置项值仅能为以下值: s, ms, us, ns";
    final static String ERR_INVALID_CFG_FORMAT = "配置项不正确: 'format': %s, 配置项仅能为以下值: datax, opentsdb";


    public static class Job extends Reader.Job {
        private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

        private Configuration config = null;

        @Override
        public void init() {
            this.config = super.getPluginJobConf();
        }

        @Override
        public void destroy() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOGGER.info("拆分Job至 {} 个Task.", adviceNumber);
            List<Configuration> configurations = new ArrayList<>(adviceNumber);
            for (int i = 0; i < adviceNumber; i++) {
                configurations.add(this.config.clone());
            }
            return configurations;
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

        // Config ip
        protected String ip = "0.0.0.0";
        // Config port
        protected int port = 8904;
        // Config timeoutSeconds
        protected int timeoutSeconds = 10;
        // Config tenant
        protected String tenant = "influxdb";
        // Config database
        protected String database = "public";
        // Config username
        protected String username = "root";
        // Config password
        protected String password = "root";
        // Config sql
        protected String sql = "SELECT 1;";

        protected static final String HEADER_TENANT = "tenant";

        @Override
        public void init() {
            Configuration config = super.getPluginJobConf();
            this.ip = StringUtils.defaultIfBlank(config.getString(CFG_IP), this.ip);
            this.port = config.getInt(CFG_PORT, this.port);
            this.timeoutSeconds = config.getInt(CFG_TIMEOUT_SECONDS, this.timeoutSeconds);
            this.tenant = StringUtils.defaultIfBlank(config.getString(CFG_TENANT), this.tenant);
            this.database = StringUtils.defaultIfBlank(config.getString(CFG_DATABASE), this.database);
            this.username = StringUtils.defaultIfBlank(config.getString(CFG_USERNAME), this.username);
            this.password = StringUtils.defaultIfBlank(config.getString(CFG_PASSWORD), this.password);
            this.sql = StringUtils.defaultIfBlank(config.getString(CFG_SQL), this.sql);
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startRead(RecordSender recordSender) {
            Location location = Location.forGrpcInsecure(this.ip, this.port);

            try (BufferAllocator allocator = new RootAllocator();
                 FlightClient flightClient = FlightClient.builder(allocator, location).build();
                 FlightSqlClient sqlClient = new FlightSqlClient(flightClient)
            ) {
                List<CallOption> callOptions = this.buildCallOptions(flightClient);
                LOGGER.trace("Executing sql: {}", this.sql);
                try (FlightSqlClient.PreparedStatement preparedStatement =
                             sqlClient.prepare(this.sql, callOptions.toArray(new CallOption[0]))
                ) {
                    final FlightInfo info = preparedStatement.execute();

                    if (info.getEndpoints().isEmpty()) {
                        throw new Exception("No endpoints found");
                    }
                    Ticket ticket = info.getEndpoints().get(0).getTicket();

                    Optional<Schema> schemaOptional = info.getSchemaOptional();
                    if (!schemaOptional.isPresent()) {
                        throw new Exception("No schema found");
                    }
                    List<AbstractRecordWriter> writers = this.buildRecordWriters(schemaOptional.get());

                    try (FlightStream stream = sqlClient.getStream(ticket)) {
                        while (stream.next()) {
                            VectorSchemaRoot root = stream.getRoot();
                            List<FieldVector> vectors = root.getFieldVectors();
                            if (vectors.size() != writers.size()) {
                                throw new Exception(String.format(
                                        "Length of field vectors does not equal to the length of schema fields, %d != %d",
                                        vectors.size(), writers.size())
                                );
                            }
                            for (int i = 0; i < vectors.size(); i++) {
                                FieldVector vector = vectors.get(i);
                                AbstractRecordWriter writer = writers.get(i);
                                writer.reset(vector.getReader(), root.getRowCount());
                            }
                            for (int rowId = 0; rowId < root.getRowCount(); rowId++) {
                                Record record = recordSender.createRecord();
                                for (AbstractRecordWriter writer : writers) {
                                    if (!writer.writeNextRecord(record)) {
                                        LOGGER.debug("writer finished");
                                    }
                                }
                                recordSender.sendToWriter(record);
                            }

                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private List<CallOption> buildCallOptions(FlightClient flightClient) {
            List<CallOption> callOptions = new ArrayList<>();
            // Authenticate option
            Optional<CredentialCallOption> credentialCallOption =
                    flightClient.authenticateBasicToken(username, password);
            credentialCallOption.ifPresent(callOptions::add);
            // Header option
            CallHeaders headers = new FlightCallHeaders();
            headers.insert(HEADER_TENANT, tenant);
            callOptions.add(new HeaderCallOption(headers));
            // Timeout option
            callOptions.add(CallOptions.timeout(timeoutSeconds, TimeUnit.SECONDS));
            return callOptions;
        }

        private List<AbstractRecordWriter> buildRecordWriters(Schema schema) {
            List<AbstractRecordWriter> writers = new ArrayList<>();
            for (Field field : schema.getFields()) {
                writers.add(AbstractRecordWriter.build(field.getType()));
            }
            return writers;
        }
    }
}
