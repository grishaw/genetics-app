package rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.io.IOException;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.OK;
import static query.QueryEngine.EMPTY_RESULT;

public class MainTest {

    static{
        Logger.getLogger("org.apache").setLevel(Level.WARN);
    }

    //TODO add more tests + test on Tomcat (use docker?)

    public static final String REPO_PATH_RANGES = "src/test/resources/repo_ranges/";

    @Test
    public void getResultTest() throws IOException {

        //TODO set real values instead once moved completely to ranges
        System.setProperty("REPO_HG_19_PATH", REPO_PATH_RANGES);
        System.setProperty("REPO_HG_38_PATH", REPO_PATH_RANGES);
        System.setProperty("MAX_RANGE_RECORDS_IN_RESULT", "10");

        // test common valid flow
        Response response = new Main().getResult19("X:77633124");
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertNotNull(response.getEntity());
        JsonNode result = ((ArrayNode) new ObjectMapper().readTree((String)response.getEntity()).get("entries")).get(0);
        Assert.assertEquals("G", result.get("ref").asText());
        Assert.assertEquals("A", result.get("alt").asText());
        ArrayNode homArray = (ArrayNode) result.get("hom");
        Assert.assertEquals(1, homArray.size());
        Assert.assertEquals("SRR14860530", homArray.get(0).asText());
        ArrayNode hetArray = (ArrayNode) result.get("het");
        Assert.assertEquals(1, hetArray.size());
        Assert.assertEquals("SRR14860527", hetArray.get(0).asText());

        // test lower case
        response = new Main().getResult("x:77633124", REPO_PATH_RANGES, 10);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertNotNull(response.getEntity());
        result = ((ArrayNode) new ObjectMapper().readTree((String)response.getEntity()).get("entries")).get(0);
        Assert.assertEquals("G", result.get("ref").asText());
        Assert.assertEquals("A", result.get("alt").asText());
        homArray = (ArrayNode) result.get("hom");
        Assert.assertEquals(1, homArray.size());
        Assert.assertEquals("SRR14860530", homArray.get(0).asText());
        hetArray = (ArrayNode) result.get("het");
        Assert.assertEquals(1, hetArray.size());
        Assert.assertEquals("SRR14860527", hetArray.get(0).asText());

        //test range query
        response = new Main().getResult("2:25234482-26501857", REPO_PATH_RANGES, 10);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());

        JsonNode jsonResult = new ObjectMapper().readTree((String)response.getEntity());
        Assert.assertEquals(11, jsonResult.get("count").asInt());

        ArrayNode dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(10, dataArray.size());

        JsonNode first = dataArray.get(0);
        Assert.assertEquals(25234482, first.get("pos").asInt());
        Assert.assertEquals("C", ((ArrayNode)first.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("T", ((ArrayNode)first.get("entries")).get(0).get("alt").asText());

        JsonNode last = dataArray.get(9);
        Assert.assertEquals(25313958, last.get("pos").asInt());
        Assert.assertEquals("G", ((ArrayNode)last.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("A", ((ArrayNode)last.get("entries")).get(0).get("alt").asText());

        // test empty case
        response = new Main().getResult("x:15000112", REPO_PATH_RANGES, 10);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertNotNull(response.getEntity());
        Assert.assertEquals(EMPTY_RESULT, response.getEntity());

        // test bad input 1
        response = new Main().getResult("adkwjfh", REPO_PATH_RANGES, 10);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad input 2
        response = new Main().getResult("s:sss", REPO_PATH_RANGES, 10);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad input 3
        response = new Main().getResult("s:12345", REPO_PATH_RANGES, 10);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad input 4
        response = new Main().getResult("x:500000000", REPO_PATH_RANGES, 10);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());
    }

}
