package BigProfiles;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class WordCount {

	public static void main(String[] args) {
		// System.out.println("Hello World!");

		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		JavaPairRDD<String, Integer> ones = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((a, b) -> a + b);

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<String, Integer> tuple : output) {
			System.out.println(tuple._1 + ": " + tuple._2);
		}

		ctx.stop();
		ctx.close();
	}
}
