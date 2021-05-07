package query;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class QueryEngine {

    private static final Logger logger = LogManager.getLogger(QueryEngine.class);

    public static String getMutationsByIndex(String chrom, int pos, String repoPath){

        //TODO refine spark config
        SparkSession spark = SparkSession.builder().appName("genetics-app").master("local[*]").getOrCreate();

        String awsKey = System.getProperty("AWS_ACCESS_KEY_ID");
        String awsSecret = System.getProperty("AWS_SECRET_ACCESS_KEY");
        if (awsKey != null && awsSecret != null) {
            Configuration conf = spark.sparkContext().hadoopConfiguration();
            conf.set("fs.s3a.access.key", awsKey);
            conf.set("fs.s3a.secret.key", awsSecret);
        }

        String path = repoPath + String.format("chrom=%s/pos_bucket=%d/", "chr" + chrom.toUpperCase(), pos % 100);

        Dataset df;

        //TODO refine
        try {
            df = spark.read().parquet(path);
        }catch(Exception e){
            logger.error(e);
            return "{}";
        }

        //TODO verify partitions are used, is 100 is optimal?
        Dataset result = df.where(col("pos").equalTo(pos)).select(to_json(struct(col("entries")))).cache();

        if (result.count() == 1) {
            return (String) result.as(Encoders.STRING()).collectAsList().get(0);
        } else if (result.count() == 0){
            logger.debug("empty result");
            return "{}";
        } else{
            logger.error("invalid state - multiple results - " + result.count());
            throw new IllegalStateException("multiple results");
        }
    }
}
