package info.sarihh.spark.unit;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import info.sarihh.kafka.nine.utils.KafkaNineUtils;

/**
 * A unit testing class for verifying the functionality of consuming messages from a Spark stream via SparkUnitRule.
 * 
 * @author Sari Haj Hussein
 */
public class SparkUnitRuleIntegrationTest {
    @ClassRule
    public static SparkUnitRule sparkUnitRule = new SparkUnitRule(60);

    private static SparkUnit sparkUnit;
    private static KafkaNineUtils<String, String> kafkaUtils;

    @SuppressWarnings("unchecked")
    @BeforeClass
    public static void setup() {
        sparkUnit = sparkUnitRule.getSparkUnit();
        kafkaUtils = KafkaNineUtils.getInstance();
        kafkaUtils.createTopic(sparkUnit.getDefaultZookeeperServer(), sparkUnit.getDefaultTopic());
        kafkaUtils.deleteTopic(sparkUnit.getDefaultZookeeperServer(), sparkUnit.getNonExistingTopic());
    }

    @AfterClass
    public static void cleanup() {
        kafkaUtils.deleteTopic(sparkUnit.getDefaultZookeeperServer(), sparkUnit.getDefaultTopic());
    }

    @Test
    public void successfullyCreateSparkStreamFromSparkUnitRule() {
        JavaStreamingContext jssc = sparkUnit.getJssc();
        JavaPairInputDStream<String, String> tuples = sparkUnit.getTuples();
        tuples.print();
        Assert.assertNotNull("SparkUnit instance should not be null.", sparkUnit);
        Assert.assertNotNull("JavaStreamingContext instance should not be null.", jssc);
        Assert.assertNotNull("JavaPairInputDStream instance should not be null.", tuples);
        Assert.assertTrue("SparkUnit instance should be of the right type.", sparkUnit instanceof SparkUnit);
        Assert.assertTrue("JavaStreamingContext instance should be of the right type.", jssc instanceof JavaStreamingContext);
        Assert.assertTrue("JavaPairInputDStream instance should be of the right type.", tuples instanceof JavaPairInputDStream<?, ?>);
    }

    @Test
    public void successfullyConsumeFromSparkStream() {
        JavaPairInputDStream<String, String> tuples = sparkUnit.getTuples();
        tuples.print();
        JavaDStream<String> lines = tuples.map(SparkFunctions.GET_TUPLE_SECOND);
        lines.print();
        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            private static final long serialVersionUID = -3041066139179726816L;

            @Override
            public void call(JavaRDD<String> args) {
                List<String> consumedMessageList = args.collect();
                if (consumedMessageList.size() != 0) {
                    String consumedMessage = args.collect().get(0);
                    Assert.assertEquals("The published message should match the consumed message.", "Spark Unit is fun!", consumedMessage);
                }
            }
        });
    }
}