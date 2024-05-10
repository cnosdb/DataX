package com.alibaba.datax.plugin.reader.influxdbreader;

import junit.framework.TestCase;
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
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMicroHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class InfluxDBReaderTest extends TestCase {
    public void testFlightSql() {
        Location location = Location.forGrpcInsecure("0.0.0.0", 8904);

        try (BufferAllocator allocator = new RootAllocator();
             FlightClient flightClient = FlightClient.builder(allocator, location).build();
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)
        ) {

            List<CallOption> callOptions = new ArrayList<>();
            // Authenticate option
            Optional<CredentialCallOption> credentialCallOption =
                    flightClient.authenticateBasicToken("root", "");
            credentialCallOption.ifPresent(callOptions::add);
            // Header option
            final CallHeaders headers = new FlightCallHeaders();
            headers.insert("tenant", "influxdb");
            callOptions.add(new HeaderCallOption(headers));
            // Timeout option
            callOptions.add(CallOptions.timeout(2, TimeUnit.SECONDS));

            try (final FlightSqlClient.PreparedStatement preparedStatement =
                         sqlClient.prepare("SELECT time, ta, fa, fb, fc, fd from ma;", callOptions.toArray(new CallOption[0]))
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
                Schema schema = schemaOptional.get();
                for (Field field : schema.getFields()) {
                    System.out.printf("Field: %s, type: %s\n", field.getName(), field.getType().toString());
                }

                try (FlightStream stream = sqlClient.getStream(ticket)) {
                    int columnIndex = 0;
                    while (stream.next()) {
                        if (!stream.hasRoot()) {
                            continue;
                        }
                        List<FieldVector> vectors = stream.getRoot().getFieldVectors();
                        for (FieldVector vector : vectors) {
                            ArrowType arrowType = vector.getField().getType();
                            System.out.printf("Vector: %s, type: %s\n", vector.getName(), arrowType.toString());
                            FieldReader fieldReader = vector.getReader();
                            if (arrowType instanceof ArrowType.Timestamp) {
                                ArrowType.Timestamp type = (ArrowType.Timestamp) arrowType;
                                switch (type.getUnit()) {
                                    case SECOND:
                                        NullableTimeStampSecHolder secHolder = new NullableTimeStampSecHolder();
                                        for (int i = 0; i < vector.getValueCount(); i++) {
                                            fieldReader.setPosition(i);
                                            if (fieldReader.isSet()) {
                                                fieldReader.read(secHolder);
                                                System.out.println(secHolder.value);
                                            } else {
                                                System.out.println("NULL");
                                            }
                                        }
                                        break;
                                    case MILLISECOND:
                                        NullableTimeStampMilliHolder milliHolder = new NullableTimeStampMilliHolder();
                                        for (int i = 0; i < vector.getValueCount(); i++) {
                                            fieldReader.setPosition(i);
                                            if (fieldReader.isSet()) {
                                                fieldReader.read(milliHolder);
                                                System.out.println(milliHolder.value);
                                            } else {
                                                System.out.println("NULL");
                                            }
                                        }
                                        break;
                                    case MICROSECOND:
                                        NullableTimeStampMicroHolder microHolder = new NullableTimeStampMicroHolder();
                                        for (int i = 0; i < vector.getValueCount(); i++) {
                                            fieldReader.setPosition(i);
                                            if (fieldReader.isSet()) {
                                                fieldReader.read(microHolder);
                                                System.out.println(microHolder.value);
                                            } else {
                                                System.out.println("NULL");
                                            }
                                        }
                                        break;
                                    case NANOSECOND:
                                        NullableTimeStampNanoHolder nanoHolder = new NullableTimeStampNanoHolder();
                                        for (int i = 0; i < vector.getValueCount(); i++) {
                                            fieldReader.setPosition(i);
                                            if (fieldReader.isSet()) {
                                                fieldReader.read(nanoHolder);
                                                System.out.println(nanoHolder.value);
                                            } else {
                                                System.out.println("NULL");
                                            }
                                        }
                                        break;
                                }
                            } else if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
                                for (int i = 0; i < vector.getValueCount(); i++) {
                                    fieldReader.setPosition(i);
                                    if (fieldReader.isSet()) {
                                        System.out.println(fieldReader.readText());
                                    } else {
                                        System.out.println("NULL");
                                    }
                                }
                            } else if (arrowType instanceof ArrowType.FloatingPoint) {
                                NullableFloat8Holder float8Holder = new NullableFloat8Holder();
                                for (int i = 0; i < vector.getValueCount(); i++) {
                                    fieldReader.setPosition(i);
                                    if (fieldReader.isSet()) {
                                        fieldReader.read(float8Holder);
                                        System.out.println(float8Holder.value);
                                    } else {
                                        System.out.println("NULL");
                                    }
                                }
                            } else if (arrowType instanceof ArrowType.Int) {
                                NullableIntHolder intHolder = new NullableIntHolder();
                                for (int i = 0; i < vector.getValueCount(); i++) {
                                    fieldReader.setPosition(i);
                                    if (fieldReader.isSet()) {
                                        fieldReader.read(intHolder);
                                        System.out.println(intHolder.value);
                                    } else {
                                        System.out.println("NULL");
                                    }

                                }
                            } else if (arrowType instanceof ArrowType.Bool) {
                                NullableBitHolder bitHolder = new NullableBitHolder();
                                for (int i = 0; i < vector.getValueCount(); i++) {
                                    fieldReader.setPosition(i);
                                    if (fieldReader.isSet()) {
                                        fieldReader.read(bitHolder);
                                        System.out.println(bitHolder.value);
                                    } else {
                                        System.out.println("NULL");
                                    }
                                }
                            } else {
                                System.out.printf("Unsupported arrow type: %s\n", arrowType);
                            }
                        }
                        System.out.println("----------");
                        columnIndex++;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
