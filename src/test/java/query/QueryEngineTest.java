package query;

import org.junit.Assert;
import org.junit.Test;

public class QueryEngineTest {

    @Test
    public void getMutationsByIndexTest(){

        // chrom=2
        String result = QueryEngine.getMutationsByIndex("2", 25211602, "src/test/resources/repo/");
        Assert.assertNotNull(result);
        System.out.println(result);

        // chrom=X
        result = QueryEngine.getMutationsByIndex("x", 114594820, "src/test/resources/repo/");
        Assert.assertNotNull(result);
        System.out.println(result);
        
    }

}
