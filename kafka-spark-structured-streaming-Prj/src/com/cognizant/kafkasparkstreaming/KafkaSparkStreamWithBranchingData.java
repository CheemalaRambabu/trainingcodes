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

public class KafkaSparkStreamWithBranchingData {

        public static void main(String[] args) {

                SparkSession spark = SparkSession.builder().appName("data-set-streaming-app").master("local[*]").getOrCreate();
                spark.sparkContext().setLogLevel("WARN");
                Dataset<Row> ds1 = spark.readStream().format("kafka").option("kafka.bootstrap.servers", 
                                "localhost:9092")
                                .option("subscribe", "test-json-topic-1").load().selectExpr("CAST(value AS STRING)");
                Dataset<Row> jsonDS=ds1.select(from_json(col("value"),Utility.employeeSchema()).as("data"));
                Dataset<Row> empDS=jsonDS.select("data.*");
                Dataset<Row> devDS=empDS.where("designation='Developer'").select("id","name");
                Dataset<Row> acctDS=empDS.where("designation='Accountant'").select("id","name");
                Dataset<Row> architectDS=empDS.where("designation='Architect'").select("id","name");
                Dataset<Row> otherDS=empDS.
                                where("designation!='Developer' and designation!='Architect' and designation!='Accountant'").
                                                select("id","name");
                 StreamingQuery devStreamingQuery = null;
                 StreamingQuery acctStreamingQuery = null;
                 StreamingQuery architectStreamingQuery = null;
                 StreamingQuery otherStreamingQuery = null;
                try {
                        devStreamingQuery = devDS.writeStream().trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                                        .format("csv")
                                        .option("checkpointLocation","c:/devcheckpoint")
                                        .option("header", true)
                                        .start("c:/structuredstream-kafka-dev-out");
                        acctStreamingQuery = acctDS.writeStream().trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                                        .format("json")
                                        .option("checkpointLocation","c:/acctcheckpoint")
                                        .option("header", true)
                                        .start("c:/structuredstream-kafka-acct-out");
                        
                        architectStreamingQuery = architectDS.writeStream().trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                                        .format("avro")
                                        .option("checkpointLocation","c:/architectcheckpoint")
                                        .option("header", true)
                                        .start("c:/structuredstream-kafka-architect-out");
                        
                        otherStreamingQuery = otherDS.writeStream().trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                                        .format("parquet")
                                        .option("checkpointLocation","c:/othercheckpoint")
                                        .option("header", true)
                                        .start("c:/structuredstream-kafka-other-out");
                        System.out.println("streaming started");

                        Thread.sleep(10 * 60 * 1000);
                        devStreamingQuery.stop();
                        acctStreamingQuery.stop();
                        architectStreamingQuery.stop();
                        otherStreamingQuery.stop();
                        
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
