package info.sarihh.spark.unit;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import info.sarihh.kafka.nine.utils.KafkaNineUtils;

/**
 * A unit testing class for verifying the functionality of consuming messages from a Spark stream via SparkUnit.
 * 
 * @author Sari Haj Hussein
 */
public class SparkUnitIntegrationTest {
    private static SparkUnit sparkUnit;
    private static KafkaNineUtils<String, String> kafkaUtils;

    @SuppressWarnings("unchecked")
    @BeforeClass
    public static void setup() {
        sparkUnit = SparkUnit.getInstance();
        kafkaUtils = KafkaNineUtils.getInstance();
        kafkaUtils.createTopic(sparkUnit.getDefaultZookeeperServer(), sparkUnit.getDefaultTopic());
        kafkaUtils.deleteTopic(sparkUnit.getDefaultZookeeperServer(), sparkUnit.getNonExistingTopic());
    }

    @AfterClass
    public static void cleanup() {
        kafkaUtils.deleteTopic(sparkUnit.getDefaultZookeeperServer(), sparkUnit.getDefaultTopic());
    }

    @Test
    public void successfullyConsumeFromSparkStream() {
        sparkUnit.createSparkStreamFromKafka(sparkUnit.getDefaultSparkApplicationName(), sparkUnit.getDefaultSparkMasterUrl(),
            sparkUnit.getDefaultBatchDuration(), sparkUnit.getDefaultTopic(), sparkUnit.getDefaultKafkaServer());
        JavaPairInputDStream<String, String> tuples = sparkUnit.getTuples();
        tuples.print();
        JavaDStream<String> lines = tuples.map(SparkFunctions.GET_TUPLE_SECOND);
        lines.print();
        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            private static final long serialVersionUID = 1407665390706698674L;

            @Override
            public void call(JavaRDD<String> args) {
                List<String> consumedMessageList = args.collect();
                if (consumedMessageList.size() != 0) {
                    String consumedMessage = args.collect().get(0);
                    Assert.assertEquals("The published message should match the consumed message.", "Spark Unit is fun!", consumedMessage);
                }
            }
        });
        sparkUnit.startWithTimeout(60);
    }
}