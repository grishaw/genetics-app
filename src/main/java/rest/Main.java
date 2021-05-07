package rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.OK;


@Path("/index")
public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);

    @GET
    @Path("/{index}")
    public Response getStatus(@PathParam("index") String index) {
        logger.debug("Got request with index: " + index);
        return Response.status(OK).entity("your index is - " + index).build();
    }
}
