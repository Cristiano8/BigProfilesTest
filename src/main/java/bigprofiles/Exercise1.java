package bigprofiles;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Exercise1 {
	
	public static void main(String[] args) {

		String outputPath = args[1];
		
		//Inizializza lo Spark Context e prendi in input il file 
		SparkConf sparkConf = new SparkConf().setAppName("Exercise1").setMaster("local[4]");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> text = ctx.textFile(args[0], 1);
		
		//Creo RDD di interi, conto quanti numeri ci sono e quanto è l'1% del totale
		JavaRDD<Integer> numbers = text.map(f -> Integer.parseInt(f));
		int count = (int) numbers.count();
		double percentage = count / 100;
		//System.out.println(percentage);
		
		/*Voglio avere delle finestre dell'1%. Assegno ad ogni numero una chiave in modo 
		 * tale di avere tante chiavi dello stesso valore quanto è grande la finestra richiesta.
		 * */ 
		List<Integer> listPair = numbers.collect();
		List<Tuple2<Integer, Integer>> increment = new ArrayList<Tuple2<Integer, Integer>>();		
		int i = 0;
		for (int n: listPair) {
			int k = (int) (i/percentage);
			increment.add(new Tuple2<Integer, Integer>(k+1, n));
			i ++;
		}
		
		//Una volta assegnate le chiavi giuste eseguo la somma per finestre
		JavaRDD<Tuple2<Integer, Integer>> tupleFromList = ctx.parallelize(increment,1);
//		tupleFromList.saveAsTextFile("/opt/spark/application/test_data/temp_ouput1");
		JavaPairRDD<Integer, Integer> pairFromTuple = JavaPairRDD.fromJavaRDD(tupleFromList); 
		JavaPairRDD<Integer, Integer> pairSum = pairFromTuple.reduceByKey((a , b) -> (a + b));
//		pairSum.saveAsTextFile("/opt/spark/application/test_data/temp_ouput1");
		
		//Calcolo la somma con la precente finestra
		List<Tuple2<Integer, Integer>> listSum = pairSum.collect();
		Map<Integer, Integer> mapSum = new HashMap<Integer, Integer>();
		for (Tuple2<Integer, Integer> t: listSum) {
			mapSum.put(t._1, t._2);
		}
		
		Map<Integer, Integer> mapSumPrev = new HashMap<Integer, Integer>();
		for (int key: mapSum.keySet()) {
			if (key == 1) {
				mapSumPrev.put(key, mapSum.get(key));
			} else {
				mapSumPrev.put(key, mapSum.get(key) + mapSumPrev.get(key -1));	
			}
		}
		
//		for (int key: mapSumPrev.keySet()) {
//			System.out.println(key + ": " + mapSumPrev.get(key));
//		}
		
		//Creo file in output
		List<Tuple2<Integer, Integer>> outputList = new ArrayList<Tuple2<Integer,Integer>>();
		for (int key: mapSumPrev.keySet()) {
			outputList.add(new Tuple2<Integer, Integer>(key, mapSumPrev.get(key)));
		}
		JavaRDD<Tuple2<Integer, Integer>> output = ctx.parallelize(outputList,1);
		
		output.saveAsTextFile(outputPath);
		
		ctx.stop();
		ctx.close();

	}
}
