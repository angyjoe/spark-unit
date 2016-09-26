package info.sarihh.spark.unit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

/**
 * The main unit testing utility in the Spark Unit framework.
 * 
 * @author Sari Haj Hussein
 */
public class SparkUnit {
    private JavaStreamingContext jssc;
    private JavaPairInputDStream<String, String> tuples;

    private SparkUnit() {
        // defeat external instantiation
    }

    /** Returns an instance of the SparkUtils class. */
    public static SparkUnit getInstance() {
        return new SparkUnit();
    }

    /** Gets Spark streaming context. */
    public JavaStreamingContext getJssc() {
        return jssc;
    }

    /** Gets Spark streaming tuples. */
    public JavaPairInputDStream<String, String> getTuples() {
        return tuples;
    }

    /** Gets the default application name in Spark web UI. */
    public String getDefaultSparkApplicationName() {
        return "Spark Unit Application";
    }

    /** Gets the default Spark master URL. */
    public String getDefaultSparkMasterUrl() {
        return "local[*]";
    }

    /** Gets the default time interval at which Spark streaming data will be divided into batches. */
    public long getDefaultBatchDuration() {
        return 2L;
    }

    /** Gets the default Zookeeper server. */
    public String getDefaultZookeeperServer() {
        return "localhost:2181";
    }

    /** Gets the default Kafka server. */
    public String getDefaultKafkaServer() {
        return "localhost:9092";
    }

    /** Gets the default topic. */
    public String getDefaultTopic() {
        return "TestTopic";
    }

    /** Gets a non-existting topic. */
    public String getNonExistingTopic() {
        return "NonExistingTopic";
    }

    /** Creates a Spark stream from Kafka with the default values of Spark and Kafka configs. */
    public void createSparkStreamFromKafka() {
        SparkConf sparkConf = new SparkConf().setAppName(getDefaultSparkApplicationName()).setMaster(getDefaultSparkMasterUrl());
        jssc = new JavaStreamingContext(sparkConf, Durations.seconds(getDefaultBatchDuration()));
        Set<String> topicsSet = new HashSet<String>(Arrays.asList(getDefaultTopic()));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("bootstrap.servers", getDefaultKafkaServer());
        tuples = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
    }

    /** Creates a Spark stream from Kafka with the supplied values of Spark and Kafka configs. */
    public void createSparkStreamFromKafka(String sparkApplicationName, String sparkMasterUrl, long batchDuration, String kafkaTopics, String kafkaServer) {
        SparkConf sparkConf = new SparkConf().setAppName(sparkApplicationName).setMaster(sparkMasterUrl);
        jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
        Set<String> topicsSet = new HashSet<String>(Arrays.asList(kafkaTopics.split(",")));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("bootstrap.servers", kafkaServer);
        tuples = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
    }

    /** Starts Spark streaming context without timeout. */
    public void startWithoutTimeout() {
        jssc.start();
        jssc.awaitTermination();
    }

    /** Starts Spark streaming context and timeouts after the specified number of seconds. */
    public void startWithTimeout(long seconds) {
        jssc.start();
        jssc.awaitTerminationOrTimeout(1000 * seconds);
    }

    /** Gracefully stops Spark streaming context. */
    public void stop() {
        jssc.stop(true, true);
    }
}