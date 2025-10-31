package com.cognizant.kafkasparkstreaming;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import static org.apache.spark.sql.functions.*;

public class WriteToKafka {
public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("data-set-streaming-app")
                                                .master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        Dataset<Row> dataset1=spark.readStream().schema(Utility.employeeSchema())
                                        .csv("C:/structuredstreaminput/test").where("designation='Developer'");
        try {
                
                Dataset<Row> dataset2=dataset1.select(col("name").cast("string").alias("value"));
                StreamingQuery streamingQuery=dataset2.writeStream()
                                .trigger(Trigger.ProcessingTime(30,TimeUnit.SECONDS))
                                .format("kafka")
                                .option("kafka.bootstrap.servers", "localhost:9092")
                                .option("topic", "test-consumer-topic-1")
                                .option("checkPointLocation", "c:/kafka-out-checkpoint")
                                .start();
                System.out.println("streaming started");
                Thread.sleep(10*60*1000);
                streamingQuery.stop();
        } catch (TimeoutException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
        } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
        }
        
}
}