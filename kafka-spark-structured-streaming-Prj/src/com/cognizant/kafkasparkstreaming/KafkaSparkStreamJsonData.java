package com.cognizant.kafkasparkstreaming;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import static org.apache.spark.sql.functions.*;

public class KafkaSparkStreamJsonData {

        public static void main(String[] args) {

                SparkSession spark = SparkSession.builder().appName("data-set-streaming-app").master("local[*]").getOrCreate();
                spark.sparkContext().setLogLevel("WARN");
                Dataset<Row> ds1 = spark.readStream().format("kafka").option("kafka.bootstrap.servers", 
                                "localhost:9092")
                                .option("subscribe", "test-json-topic-1").load().selectExpr("CAST(value AS STRING)");
                Dataset<Row> jsonDS=ds1.select(from_json(col("value"),Utility.employeeSchema()).as("data"));
                Dataset<Row> empDS=jsonDS.select("data.*");
                 StreamingQuery streamingQuery = null;
                try {
                        streamingQuery = empDS.writeStream().trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                                        .format("console").start();

                        System.out.println("streaming started");

                        Thread.sleep(10 * 60 * 1000);
                        streamingQuery.stop();
                } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                } catch (TimeoutException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }

                System.out.println("streaming stopped");
        }

}