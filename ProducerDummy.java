package objects;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDummy {

	public ProducerDummy() {
		super();
	}

	public void pushMessage(String symbol) throws IOException {
		// properties for producer
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.2.19:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		String html = "";
		String sbl = symbol;
		String price = "";
		String timeClosedPrice = "";
		String previousClosedPrice = "";
		String consoMge = "";

		// create producer
		Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

		// reading data from url
		StockQuote sQuote = new StockQuote();
		html = sQuote.readHTML(sbl);
		price = sQuote.getPrice(html);
		timeClosedPrice = sQuote.getDate(html).toLowerCase();
		previousClosedPrice = sQuote.getPreviousClose(html);
		consoMge = consoMge + "Tesla stock symbol: " + sbl + "\nStock price: " + price + "\nTime of the stock price: "
				+ timeClosedPrice + "\nPrevious closed price: " + previousClosedPrice + "\n";

		// send messages to topic
		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("KafkaTestTopic", 0,
				consoMge);
		producer.send(producerRecord);

		// close producer
		producer.close();
	}
}
