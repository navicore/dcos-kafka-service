package com.mesosphere.dcos.kafka.web;

import com.mesosphere.dcos.kafka.cmd.CmdExecutor;
import com.mesosphere.dcos.kafka.state.KafkaState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

@Path("/v1/topics")
public class TopicController {
  private static final Log log = LogFactory.getLog(TopicController.class);

  private final CmdExecutor cmdExecutor;
  private final KafkaState state;

  public TopicController(CmdExecutor cmdExecutor, KafkaState state) {
    this.cmdExecutor = cmdExecutor;
    this.state = state;
  }

  @GET
  public Response topics() {
    try {
      JSONArray topics = state.getTopics();
      return Response.ok(topics.toString(), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      log.error("Failed to fetch topics with exception: " + ex);
      return Response.serverError().build();
    }
  }

  @POST
  public Response createTopic(
      @QueryParam("name") String name,
      @QueryParam("partitions") String partitionCount,
      @QueryParam("replication") String replicationFactor) {

    try {
      int partCount = Integer.parseInt(partitionCount);
      int replFactor = Integer.parseInt(replicationFactor);
      JSONObject result = cmdExecutor.createTopic(name, partCount, replFactor);
      return Response.ok(result.toString(), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      log.error("Failed to create topic: " + name + " with exception: " + ex);
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/unavailable_partitions")
  public Response unavailablePartitions() {
    try {
      JSONObject obj = cmdExecutor.unavailablePartitions();
      return Response.ok(obj.toString(), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      log.error("Failed to fetch topics with exception: " + ex);
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/under_replicated_partitions")
  public Response underReplicatedPartitions() {
    try {
      JSONObject obj = cmdExecutor.underReplicatedPartitions();
      return Response.ok(obj.toString(), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      log.error("Failed to fetch topics with exception: " + ex);
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/{name}")
  public Response getTopic(@PathParam("name") String topicName) {
    try {
      JSONObject topic = state.getTopic(topicName);
      return Response.ok(topic.toString(), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      log.error("Failed to fetch topic: " + topicName + " with exception: " + ex);
      return Response.serverError().build();
    }
  }

  @PUT
  @Path("/{name}")
  public Response operationOnTopic(
      @PathParam("name") String name,
      @QueryParam("operation") String operation,
      @QueryParam("key") String key,
      @QueryParam("value") String value,
      @QueryParam("partitions") String partitions,
      @QueryParam("messages") String messages) {

    try {
      JSONObject result = null;
      List<String> cmds = null;

      if (operation == null) {
        result = new JSONObject();
        result.put("Error", "Must designate an 'operation'.  Possibles operations are [producer-test, delete, partitions, config, deleteConfig].");
      } else {
        switch (operation) {
          case "producer-test":
            int messageCount = Integer.parseInt(messages);
            result = cmdExecutor.producerTest(name, messageCount);
            break;
          case "partitions":
            cmds = Arrays.asList("--partitions", partitions);
            result = cmdExecutor.alterTopic(name, cmds);
            break;
          default:
            result = new JSONObject();
            result.put("Error", "Unrecognized operation: " + operation);
            break;
        }
      }

      return Response.ok(result.toString(), MediaType.APPLICATION_JSON).build();

    } catch (Exception ex) {
      log.error("Failed to perform operation: " + operation + " on Topic: " + name + " with exception: " + ex);
      return Response.serverError().build();
    }
  }

  @DELETE
  @Path("/{name}")
  public Response deleteTopic(
      @PathParam("name") String name) {

    try {
      JSONObject result = cmdExecutor.deleteTopic(name);
      String message = result.getString("message");
      if (message.contains("This will have no impact if delete.topic.enable is not set to true")) {
        return Response.accepted().entity(result.toString()).type(MediaType.APPLICATION_JSON).build();
      } else {
        return Response.ok(result.toString(), MediaType.APPLICATION_JSON).build();
      }

    } catch (Exception ex) {
      log.error("Failed to delete Topic: " + name + " with exception: " + ex);
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/{name}/offsets")
  public Response getOffsets(@PathParam("name") String topicName, @QueryParam("time") Long time) {
    try {
      JSONArray offsets = cmdExecutor.getOffsets(topicName, time);
      return Response.ok(offsets.toString(), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      log.error("Failed to fetch offsets for: " + topicName + " with exception: " + ex);
      return Response.serverError().build();
    }
  }
}
