package info.sarihh.spark.unit;

import org.junit.rules.ExternalResource;

/**
 * A JUnit rule for Spark Unit which automatically starts the servers and Spark processing.
 * 
 * @author Sari Haj Hussein
 */
public class SparkUnitRule extends ExternalResource {
    private final long timeoutSeconds;
    private final SparkUnit sparkUnit;

    public SparkUnitRule(long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
        this.sparkUnit = SparkUnit.getInstance();
        sparkUnit.createSparkStreamFromKafka();
    }

    public SparkUnitRule(String sparkApplicationName, String sparkMasterUrl, long batchDuration, String kafkaTopics, String kafkaServer, long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
        this.sparkUnit = SparkUnit.getInstance();
        sparkUnit.createSparkStreamFromKafka(sparkApplicationName, sparkMasterUrl, batchDuration, kafkaTopics, kafkaServer);
    }

    public SparkUnit getSparkUnit() {
        return sparkUnit;
    }

    @Override
    protected void before() throws Throwable {
        super.before();
    }

    @Override
    protected void after() {
        if (timeoutSeconds == 0) {
            sparkUnit.startWithoutTimeout();
        } else {
            sparkUnit.startWithTimeout(timeoutSeconds);
        }
    }
}
