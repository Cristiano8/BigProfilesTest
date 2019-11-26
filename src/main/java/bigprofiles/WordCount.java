package bigprofiles;

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
		
//		int i = 1;
//		int j = 1;
//		for (Integer n : listPair) {
//			increment.add(new Tuple2<Integer, Integer>(i, n));
//			j ++;
//			if (j > percentage) {
//				j = 1;
//				i ++;
//			}
//		}

//		JavaPairRDD<String, Double> outputRDD = idDistance.mapToPair(
//		t -> new Tuple2<Double, String>(t._2, t._1)).sortByKey(false).mapToPair(
//		t -> new Tuple2<String, Double>(t._2, t._1));


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
