package info.sarihh.spark.unit;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * A set of Spark Streaming transformation functions that define the computations on Spark discretized streams.
 *
 * @author Sari Haj Hussein
 */
public class SparkFunctions {
    /** Gets the first element in a Scala tuple. */
    public static Function<Tuple2<String, String>, String> GET_TUPLE_FIRST = new Function<Tuple2<String, String>, String>() {
        private static final long serialVersionUID = -1880764028674143088L;

        @Override
        public String call(Tuple2<String, String> tuple) {
            return tuple._1();
        }
    };

    /** Gets the second element in a Scala tuple. */
    public static Function<Tuple2<String, String>, String> GET_TUPLE_SECOND = new Function<Tuple2<String, String>, String>() {
        private static final long serialVersionUID = -3070772625892219876L;

        @Override
        public String call(Tuple2<String, String> tuple) {
            return tuple._2();
        }
    };
}