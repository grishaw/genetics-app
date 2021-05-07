package rest;

import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.Status.*;

public class MainTest {

    //TODO add real tests + test on Tomcat (use docker?)

    public static final String REPO_PATH = "src/test/resources/repo/";

    @Test
    public void testMain(){

        // test common valid flow
        Response response = new Main().getResult("X:114594820", REPO_PATH);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertNotNull(response.getEntity());

        // test lower case
        response = new Main().getResult("x:114594820", REPO_PATH);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertNotNull(response.getEntity());

        // test empty case
        response = new Main().getResult("x:111", REPO_PATH);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertNotNull(response.getEntity());

        // test bad input 1
        response = new Main().getResult("adkwjfh", REPO_PATH);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad input 2
        response = new Main().getResult("s:sss", REPO_PATH);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());
    }
}
