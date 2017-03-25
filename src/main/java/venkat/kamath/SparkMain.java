package venkat.kamath;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by venkat kamath on 25/03/17.
 */
public class SparkMain {

    private JavaSparkContext jsc;

    public SparkMain() {
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark Play");
        conf.setMaster("local");
        jsc = new JavaSparkContext(conf);
    }


    public void close() {
        jsc.close();
    }

    public static void main(String[] args) throws InterruptedException {
        SparkMain driver = new SparkMain();

        driver.expMapAndFilter();

        synchronized (Thread.currentThread()) {
            Thread.currentThread().wait();
        }
        driver.close();

    }

    public void expMapAndFilter() {
        JavaRDD<Integer> numbersRDD = jsc.parallelize( Arrays.asList(1,2,3) );
        JavaRDD<Integer> squaresRDD = numbersRDD.map( n -> n * n);
        System.out.println(squaresRDD.collect());

        JavaRDD<Integer> evenRDD = squaresRDD.filter(n -> n % 2 == 0);
        System.out.println(evenRDD.collect());

        JavaRDD<Integer> flatNumbersJavaRDD = numbersRDD.flatMap(n -> Arrays.asList(n, n * 2, n * 3).iterator());
        System.out.println(flatNumbersJavaRDD.collect());
    }
}
