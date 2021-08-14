package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class ImportVcfToDataLake {


    public static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];

        SparkSession sparkSession = SparkSession.builder().appName(ImportVcfToDataLake.class.getName()).getOrCreate();

        Dataset result = convertVcfsToDatalakeFormat(sparkSession, inputPath);

        writeToDataLake(result, outputPath);


    }

    static Dataset convertVcfsToDatalakeFormat(SparkSession spark, String inputPath){

        Dataset table = getMutationsByIndex(spark, inputPath);

        table = table
                .withColumn("pos_bucket", col("pos").mod(lit(100)))
                .groupBy("chrom", "pos_bucket", "pos").agg(collect_set(col("resp")).as("entries"));

        return table;
    }

    static Dataset getMutationsByIndex(SparkSession spark, String inputPath){

        Dataset raw = spark.read().textFile(inputPath).where("not value like '#%'");

        Dataset table = spark.read().option("sep", "\t").csv(raw);

        table = table
                .withColumnRenamed("_c0", "chrom")
                .withColumnRenamed("_c1", "pos")
                .withColumnRenamed("_c3", "ref")
                .withColumnRenamed("_c4", "alt");

        table = table
                .withColumn("homo", when(col("_c9").startsWith("1/1"), true).otherwise(false))
                .withColumn("srr", split(reverse(split(input_file_name(), "/")).getItem(0), "\\.").getItem(0))
                .withColumn("chrom", split(col("chrom"), "_").getItem(0))
                .withColumn("pos", col("pos").cast(DataTypes.IntegerType));

        table = table.drop("_c2", "_c5", "_c6", "_c7", "_c8", "_c9");

        table = table
                .withColumn("homo-srr", when(col("homo"), col("srr")))
                .withColumn("hetero-srr", when(not(col("homo")), col("srr")))
                .drop("homo");

        table =  table
                .groupBy("chrom", "pos","ref","alt")
                .agg(collect_set("homo-srr").as("hom"), (collect_set("hetero-srr").as("het")));

        table = table
                .withColumn("resp", struct("ref", "alt", "hom", "het"))
                .drop("hom", "het");

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
