//### www.hadoopinrealworld ###
//### Spark Developer In Real World ###
//### Streaming Meetup.com with Spark ###

package com.hirw.spark

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{col, lit, concat, from_json}
import org.apache.spark.sql.types.{StructType, StringType}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord }

object Meetup {

  def main(args: Array[String]) {
    
    val spark = SparkSession
      .builder()
      .appName("Meetup")
      .getOrCreate()

    val schemaString = """{
				"type": "record",
				"name": "meetuprecord",
				"fields": [{"name":  "rsvp", "type": "string"}]
				}""""

    //Load from Kafka topic
    val load = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-41-26.ec2.internal:9092")
      .option("subscribe", "hirw-meetup-topic-serial")
      .option("startingOffsets", "earliest")
      .option("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer.class")
      .load()

    //Convert RSVP from ByteArray to String
    import spark.implicits._
    val df = load.select(col("value").as[Array[Byte]])
      .map(d => {
        val rec = new GenericDatumReader[GenericRecord](new Schema.Parser().parse(schemaString))
          .read(null, DecoderFactory.get().binaryDecoder(d, null))
        val rsvp = rec.get("rsvp").asInstanceOf[org.apache.avro.util.Utf8].toString
        rsvp
      })

    //Schema for RSVPs
    val rsvpSchema = new StructType().add("response", StringType)
      .add("event", new StructType().add("event_name", StringType).add("event_id", StringType))

    val rsvps = df.select(from_json(col("value"), rsvpSchema, Map.empty[String, String]) as "json")
      .select(col("json.response"), col("json.event.event_id"), col("json.event.event_name"))

    val rsvp_responses = rsvps.groupBy("event_id", "event_name", "response").count()

    val meetup_key_val = rsvp_responses.withColumn("key", lit("100")).select(col("key"), 
        concat(col("event_id"), lit(" | "), col("event_name"), lit(" | "), col("response"), lit(" | "), col("count")).alias("value"))

    //Writing output to Kafka Sink
    val result = meetup_key_val.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-41-26.ec2.internal:9092")
      .option("topic", "hirw-meetup-sink")
      .option("checkpointLocation", "/user/hirw/stream/chkpt")
      .outputMode("update")
      .start()
      
      result.awaitTermination()

  }

}