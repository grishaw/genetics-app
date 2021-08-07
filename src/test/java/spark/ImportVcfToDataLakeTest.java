package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class ImportVcfToDataLakeTest {

    static{
        Logger.getLogger("org.apache").setLevel(Level.WARN);
    }

    @Test
    public void convertVcfsToDatalakeFormatTest(){

        SparkSession spark = SparkSession.builder().appName("importVcfToDataLakeTest").master("local[*]").getOrCreate();

        Dataset result19 = ImportVcfToDataLake.convertVcfsToDatalakeFormat(spark, "src/test/resources/input/*/hg19/");

        Assert.assertEquals(1622, result19.count());

        result19.printSchema();

        result19.show();

        // TODO add tests

    }

    @Test
    public void writeToDataLakeTest(){

        SparkSession spark = SparkSession.builder().appName("importVcfToDataLakeTest").master("local[*]").getOrCreate();

        Dataset result38 = ImportVcfToDataLake.convertVcfsToDatalakeFormat(spark, "src/test/resources/input/*/hg38/");

        String outputPath = "target/test-out/" + UUID.randomUUID();

        ImportVcfToDataLake.writeToDataLake(result38, outputPath);

        Dataset resultFromDisk = spark.read().parquet(outputPath);

        Assert.assertEquals(result38.count(), resultFromDisk.count());

        resultFromDisk.printSchema();

        resultFromDisk.show();

        // TODO add tests

    }
}
