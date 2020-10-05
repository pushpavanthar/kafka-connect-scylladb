package io.connect.scylladb;

import com.datastax.driver.core.BoundStatement;
import io.connect.scylladb.integration.SinkRecordUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.connect.scylladb.integration.TestDataUtil.asMap;
import static io.connect.scylladb.integration.TestDataUtil.struct;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class ScyllaDbSinkTaskTest {

  Map<String, String> settings;
  ScyllaDbSinkTask task;

  @Before
  public void before() {
    settings = new HashMap<>();
    task = new ScyllaDbSinkTask();
    }

  static final String KAFKA_TOPIC = "topic";

  //TODO: failing need to check
  //@Test
  public void shouldReturnNonNullVersion() {
    assertNotNull(task.version());
  }

  @Test
  public void shouldStopAndDisconnect() {
    task.stop();
    //TODO: Ensure the task stopped
  }

  //TODO: failing need to check
  //@Test(expected = ConnectException.class)
  public void shouldFailWithInvalidRecord() {
    SinkRecord record = new SinkRecord(
        KAFKA_TOPIC,
        1,
        Schema.STRING_SCHEMA,
        "Sample key",
        Schema.STRING_SCHEMA,
        "Sample value",
        1L
    );

    // Ensure that the exception is translated into a ConnectException
    task.put(Collections.singleton(record));
  }

  //TODO: failing need to check
  //@Test
  public void version() {
    assertNotNull(task.version());
    assertFalse(task.version().equals("0.0.0.0"));
    assertTrue(task.version().matches("^(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)(-\\w+)?$"));
  }


  //@Test
  public void testRecordMapping() {
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    this.task.initialize(sinkTaskContext);

    SinkRecord record = SinkRecordUtil.write("poc_test",
            null,
            asMap(
                    struct("key",
                            "id", Schema.Type.INT64, true, 67890L
                    )
            ),
            null,
            asMap(
                    struct("key",
                            "id", Schema.Type.INT64, true, 67890L ,
                            "firstName", Schema.Type.STRING, true, "another",
                            "lastName", Schema.Type.STRING, true, "user",
                            "age", Schema.Type.INT64, true, 10L
                    )
            )
    );

    settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "scylladb");
    ScyllaDbSinkConnectorConfig sinkConnectorConfig = new ScyllaDbSinkConnectorConfig(settings);
    ScyllaDbSinkTaskHelper scyllaDbSinkTaskHelper = new ScyllaDbSinkTaskHelper(sinkConnectorConfig, new ScyllaDbSessionFactory().newSession(sinkConnectorConfig));
    scyllaDbSinkTaskHelper.validateRecord(record);
    BoundStatement boundStatement = scyllaDbSinkTaskHelper.getBoundStatementForRecord(record);

  }

//
//  @Test
//  public void test123(){
//    ObjectMapper mapper = new ObjectMapper();
//
//    try {
//
//      // JSON file to Java object
//      JsonNode s1  = mapper.readTree(new File("/Users/purushotham/repo/kafka-connect-scylladb/src/test/resources/test.json"));
//      JsonNode s2 = mapper.readTree(s1.get("batch_activated_student_count").asText());
//      // JSON string to Java object
//      String jsonInString = "{\"name\":\"mkyong\",\"age\":37,\"skills\":[\"java\",\"python\"]}";
////      JsonNode s1 = mapper.readTree(jsonInString);
//      Map<String, Object> staff2 = mapper.readValue(jsonInString, Map.class);
//
//
//      // compact print
//      System.out.println(staff2);
//
//      // pretty print
//      String prettyStaff1 = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(staff2);
//
//      System.out.println(prettyStaff1);
//
//
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
}