package rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import query.QueryEngine;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.OK;


@Path("/index")
public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);

    @GET
    @Path("/hg38/{index}")
    public Response getResult38(@PathParam("index") String index) {
        String path = System.getProperty("REPO_HG_38_PATH");
        logger.debug("hg38 request, using path - " + path);
        if (path == null){
            throw new RuntimeException("empty repo path");
        }
        return getResult(index, path);
    }

    @GET
    @Path("/hg19/{index}")
    public Response getResult19(@PathParam("index") String index) {
        String path = System.getProperty("REPO_HG_19_PATH");
        logger.debug("hg19 request, using path - " + path);
        if (path == null){
            throw new RuntimeException("empty repo path");
        }
        return getResult(index, path);
    }

    Response getResult(String index, String repoPath){

        logger.debug("Got request for index: " + index + " from path " + repoPath);

        String[] indexSplit = index.split(":");

        if (indexSplit.length != 2){
            return Response.status(BAD_REQUEST).build();
        }else {
            //TODO validate chrom (1-23, X, Y, M)
            String chrom = indexSplit[0].toLowerCase().trim();
            int pos;
            try {
                pos = Integer.parseInt(indexSplit[1].trim());
            }catch(Exception e){
                return Response.status(BAD_REQUEST).build();
            }
            logger.debug("chrom = " + chrom + ", pos = " + pos);

            String result = QueryEngine.getMutationsByIndex(chrom, pos, repoPath);
            return Response.status(OK).entity(result).build();
        }

    }

}
