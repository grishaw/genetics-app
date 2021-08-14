package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static spark.ImportVcfToDataLake.getMutationsByIndex;

public class ImportVcfToDataLakeByRanges {


    public static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];

        SparkSession sparkSession = SparkSession.builder().appName(ImportVcfToDataLakeByRanges.class.getName()).getOrCreate();

        Dataset result = convertVcfsToDatalakeFormatByRanges(sparkSession, inputPath);

        writeToDataLake(result, outputPath);


    }

    static Dataset convertVcfsToDatalakeFormatByRanges(SparkSession spark, String inputPath){

        Dataset table = getMutationsByIndex(spark, inputPath);

        table = table
                .withColumn("pos_bucket", floor(col("pos").divide(lit(1_000_000))))
                .groupBy("chrom", "pos_bucket", "pos").agg(collect_set(col("resp")).as("entries"));

        return table;
    }


    static void writeToDataLake(Dataset df, String outputPath){

        df
                .repartition(col("chrom"), col("pos_bucket"))
                .write().mode("overwrite")
                .partitionBy("chrom", "pos_bucket")
                .parquet(outputPath);


    }

}
