package biz.svyatoslav.learning.bigdata.elasticsearch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

import java.util.Arrays;
import java.util.List;

// 0) Compile it with Java 11 (11.0.21). Ignore warnings like "WARNING: An illegal reflective access operation has occurred".
// 1) Check the IP of your Elasticsearch docker container!
// 2) To check the result, go to: http://localhost:9200/people

// If you have error like " ... Exception in thread "main" java.lang.IllegalAccessError:
//  class org.apache.spark.storage.StorageUtils$ (in unnamed module ...) cannot access
//  class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does
//  not export sun.nio.ch to unnamed module ... "
// Follow this advice: https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct
// In short: add this JVM Option in IDEA IDE: "--add-exports java.base/sun.nio.ch=ALL-UNNAMED"

public class SparkJavaElasticStreamTest {

    public static void main(String[] args) {
        try {
            System.out.println("Connecting to Elasticsearch...");
            // Configure connection to Elasticsearch
            SparkSession spark = SparkSession.builder()
                    .config(ConfigurationOptions.ES_NODES, "172.29.0.2")
                    .config(ConfigurationOptions.ES_PORT, "9200")
                    .appName("StreamingElastic")
                    .master("local[*]")
                    .getOrCreate();

            System.out.println("Preparing simple data...");
            // Prepare sample data
            List<Person> data = Arrays.asList(
                    new Person("John Doe", 30),
                    new Person("Jane Doe", 25),
                    new Person("Mike Johnson", 40)
            );

            // Create DataFrame from the sample data
            Dataset<Row> df = spark.createDataFrame(data, Person.class);

            // Define Elasticsearch index
            String esIndex = "people/data";

            System.out.println("Writing simple data...");
            // Write DataFrame to Elasticsearch
            df.write()
                    .format("org.elasticsearch.spark.sql")
                    .option("es.resource", esIndex)
                    .mode(SaveMode.Append)
                    .save();

            spark.stop();
            System.out.println("Done. Visit http://localhost:9200/people and http://localhost:9200/people/_search?pretty to see the result.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Utility class to create sample data
    public static class Person {
        private String name;
        private int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        // Getters and setters
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

}