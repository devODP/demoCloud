package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import java.util.Date;
import java.text.SimpleDateFormat;


public class MultipleKafkaConsumer {
	private static final String TOPIC = "KafkaTestTopic";
	private static final String BOOTSTRAP_SERVERS = "localhost:9094";
	private static final String GROUP_ID1 = "my-consumer-group1";
	private static final String GROUP_ID2 = "my-consumer-group2";

	private static final int NUM_CONSUMERS = 2;

	public static void main(String[] args) {
		
		
		List<Double> inputData = new ArrayList<>();
		inputData.add(35.5);
		inputData.add(12.4993);
		inputData.add(90.32);
		inputData.add(20.32);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Create consumer1 properties
		Properties properties1 = new Properties();
		properties1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties1.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID1);
		properties1.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties1.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// Create consumer2 properties
		Properties properties2 = new Properties();
		properties2.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties2.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID2);
		properties2.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// Create and start multiple Kafka consumers
		Consumer<String, String> consumer1 = new KafkaConsumer<>(properties1);
		ConsumerRunnable consumerRunnable1 = new ConsumerRunnable(consumer1, TOPIC, System.out, inputData, sc);
		Thread thread1 = new Thread(consumerRunnable1);
		thread1.start();
		
		Consumer<String, String> consumer2 = new KafkaConsumer<>(properties2);
		ConsumerRunnable consumerRunnable2 = new ConsumerRunnable(consumer2, TOPIC, System.out, inputData, sc);
		Thread thread2 = new Thread(consumerRunnable2);
		thread2.start();


	}

	private static class ConsumerRunnable implements Runnable {
		private final Consumer<String, String> consumer;
		private final String topic;
		private final PrintStream ps;
		private final List<Double> data;
		private final JavaSparkContext sc;

		public ConsumerRunnable(Consumer<String, String> consumer, String topic, PrintStream printStream, List<Double> data, JavaSparkContext sc) {
			this.consumer = consumer;
			this.topic = topic;
			this.ps = printStream;
			this.sc = sc;
			this.data = data;
		}

		@Override
		public void run() {
			consumer.subscribe(Arrays.asList(topic));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					ps.println("Consumer " + Thread.currentThread().getId() + ": Received message:\n" + record.value());
					
					String [] lines = record.value().split("\n");
					String closedPriceStr = lines[1].substring(13);
					String prevClosedPriceStr = lines[3].substring(23);
					double closedPrice = Double.parseDouble(closedPriceStr);
					double prevClosedPrice = Double.parseDouble(prevClosedPriceStr);
					
					List<Double> numbers1 = Arrays.asList(closedPrice);
					List<Double> numbers2 = Arrays.asList(prevClosedPrice);
					
					JavaRDD<Double> rdd1 = sc.parallelize(numbers1);
					JavaRDD<Double> rdd2 = sc.parallelize(numbers2);
					
					JavaRDD<Double> diff = rdd1.subtract(rdd2);
					
					double result = diff.reduce((a,b)->a+b);
					
					ps.println(result);

					// formatting unique datetime as id for dynamoDB entry
					// Get the current date
					LocalDate currentDate = LocalDate.now();

					// Format the date to a string
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
					String dateString = currentDate.format(formatter);

					// updating tables in dynamoDB
					DynamoDbClient client = DynamoDbClient.builder()
                            .region(Region.US_EAST_2) // Replace with your desired region
                            .build();

                    PutItemRequest request = PutItemRequest.builder()
                            .tableName("dynamodb_table_demo")
                            .item(Map.of("id", AttributeValue.builder().s("dateString").build(),
                                         "name", AttributeValue.builder().n(result).build()))
                            .build();

					PutItemResponse response = client.putItem(request);

				}
			}
		}
	}
}
