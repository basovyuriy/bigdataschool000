package com.hirw.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.kafka.common.serialization.StringSerializer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

/**
 * 
 * @author HIRW (www.hadoopinrealworld.com)
 * Kafka Producer to Streaming live RSVPs from meetup.com to Kafka
 *
 */

public class KafkaMeetupProducerAvro {

	private final static String TOPIC = "hirw-meetup-topic-serial";
	private final static String BOOTSTRAP_SERVERS = "ip-172-31-41-26.ec2.internal:9092";

	  private static Producer<Object, Object> createProducer()
	  {
	    Properties props = new Properties();
	    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
	    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaMeetupProducer");
	    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
	    props.put("schema.registry.url", "http://localhost:8081");
	    return new KafkaProducer<Object, Object>(props);
	  }

	static void runProducer() throws Exception {
		
		Producer<Object, Object> producer = createProducer();
		
		//Schema to define the AVRO Record
		String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"meetuprecord\"," +
                "\"fields\":[{\"name\":\"rsvp\",\"type\":\"string\"}]}";
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(userSchema);
		
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		
		//HTTP connection to stream live RSVPs
		URL url = new URL("http://stream.meetup.com/2/rsvps");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		try {
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

			int msgcount = 0;
			String output;
			while ((output = br.readLine()) != null) {
				//Loop through each RSVP and create Avro Record for Value in Key-Value pair
				GenericRecord avroRecord = new GenericData.Record(schema);
				avroRecord.put("rsvp", output);
				
				//Converting AvroRecord to Byte array 
				byte[] bytes = recordInjection.apply(avroRecord);
				
				ProducerRecord<Object, Object> record = new ProducerRecord<>(TOPIC, Integer.toString(msgcount++), bytes);

				RecordMetadata metadata = (RecordMetadata) producer.send(record).get();

				//Printing the message that was sent to Kafka along with partition and offset information
				System.out.printf("sent record(key=%s value=%s) \n",
						new Object[] { record.key(), record.value()});
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			conn.disconnect();
			producer.flush();
			producer.close();
		}
	}

	public static void main(String[] args) throws Exception {
		runProducer();
	}
}
