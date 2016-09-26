package info.sarihh.spark.unit;

import org.apache.spark.SparkException;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import info.sarihh.kafka.nine.utils.KafkaNineUtils;

/**
 * A unit testing class for instantiating SparkUnit and creating a Spark stream from Kafka.
 * 
 * @author Sari Haj Hussein
 */
public class SparkUnitTest {
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

    @After
    public void stopSparkContext() {
        sparkUnit.stop();
    }

    @Test
    public void successfullyCreateSparkStreamWithoutParams() {
        sparkUnit.createSparkStreamFromKafka();
        JavaStreamingContext jssc = sparkUnit.getJssc();
        JavaPairInputDStream<String, String> tuples = sparkUnit.getTuples();
        Assert.assertNotNull("JavaStreamingContext instance should not be null.", jssc);
        Assert.assertNotNull("JavaPairInputDStream instance should not be null.", tuples);
        Assert.assertTrue("JavaStreamingContext instance should be of the right type.", jssc instanceof JavaStreamingContext);
        Assert.assertTrue("JavaPairInputDStream instance should be of the right type.", tuples instanceof JavaPairInputDStream<?, ?>);
    }

    @Test
    public void successfullyCreateSparkStreamWithParams() {
        sparkUnit.createSparkStreamFromKafka(sparkUnit.getDefaultSparkApplicationName(), sparkUnit.getDefaultSparkMasterUrl(),
            sparkUnit.getDefaultBatchDuration(), sparkUnit.getDefaultTopic(), sparkUnit.getDefaultKafkaServer());
        JavaStreamingContext jssc = sparkUnit.getJssc();
        JavaPairInputDStream<String, String> tuples = sparkUnit.getTuples();
        Assert.assertNotNull("JavaStreamingContext instance should not be null.", jssc);
        Assert.assertNotNull("JavaPairInputDStream instance should not be null.", tuples);
        Assert.assertTrue("JavaStreamingContext instance should be of the right type.", jssc instanceof JavaStreamingContext);
        Assert.assertTrue("JavaPairInputDStream instance should be of the right type.", tuples instanceof JavaPairInputDStream<?, ?>);
    }

    @Test(expected = SparkException.class)
    public void failedToCreateSparkStreamFromNonExistingKafkaTopic() {
        sparkUnit.createSparkStreamFromKafka(sparkUnit.getDefaultSparkApplicationName(), sparkUnit.getDefaultSparkMasterUrl(),
            sparkUnit.getDefaultBatchDuration(), "NonExistingTopic", sparkUnit.getDefaultKafkaServer());
        JavaStreamingContext jssc = sparkUnit.getJssc();
        JavaPairInputDStream<String, String> tuples = sparkUnit.getTuples();
        Assert.assertNotNull("JavaStreamingContext instance should not be null.", jssc);
        Assert.assertNotNull("JavaPairInputDStream instance should not be null.", tuples);
        Assert.assertTrue("JavaStreamingContext instance should be of the right type.", jssc instanceof JavaStreamingContext);
        Assert.assertTrue("JavaPairInputDStream instance should be of the right type.", tuples instanceof JavaPairInputDStream<?, ?>);
    }

    @Test(expected = SparkException.class)
    public void failedToCreateSparkStreamFromWrongKafkaServer() {
        sparkUnit.createSparkStreamFromKafka(sparkUnit.getDefaultSparkApplicationName(), sparkUnit.getDefaultSparkMasterUrl(),
            sparkUnit.getDefaultBatchDuration(), sparkUnit.getDefaultTopic(), "localhost:1000");
        JavaStreamingContext jssc = sparkUnit.getJssc();
        JavaPairInputDStream<String, String> tuples = sparkUnit.getTuples();
        Assert.assertNotNull("JavaStreamingContext instance should not be null.", jssc);
        Assert.assertNotNull("JavaPairInputDStream instance should not be null.", tuples);
        Assert.assertTrue("JavaStreamingContext instance should be of the right type.", jssc instanceof JavaStreamingContext);
        Assert.assertTrue("JavaPairInputDStream instance should be of the right type.", tuples instanceof JavaPairInputDStream<?, ?>);
    }
}
