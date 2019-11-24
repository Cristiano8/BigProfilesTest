package BigProfiles;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class Exercise2 {
	
	private static final String tableName = "rows";
	private static final String totalAverageQuery = "SELECT avg(value) FROM rows";
	private static final String idAverageQuery = "SELECT id, avg(value) FROM rows GROUP BY id";
	
	@SuppressWarnings({ "deprecation", "serial" })
	public static void main(String[] args) {
		
		String outputPath = args[1];
		
		//Inizializza lo Spark Context e prendi in input il file
		SparkConf sparkConf = new SparkConf().setAppName("Exercise2").setMaster("local[4]");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> text = ctx.textFile(args[0], 1);
		SQLContext sqlContext = new SQLContext(ctx);
		
		//Crea un RDD che abbia elementi TableRow
		JavaRDD<String> lines = text.flatMap(line -> Arrays.asList(line.split("\n")).iterator());		
		String first= lines.first();
		JavaRDD<String>	linesWithoutFirst = lines.filter(line -> !line.equals(first));
		JavaRDD<TableRow> tableRowRDD = linesWithoutFirst.map(new Function<String, TableRow>() {
			@Override
			public TableRow call(String s) throws Exception {
				String[] parts = s.split(",");
				return new TableRow(parts[0], Integer.parseInt(parts[1]));
				
			}
		});
		
		//Crea i Dataset su cui effettuare le query per la media
		Dataset<Row> df = sqlContext.createDataFrame(tableRowRDD, TableRow.class);
		df.createOrReplaceTempView(tableName);
		
		Dataset<Row> totalAverage = sqlContext.sql(totalAverageQuery);
		
		Dataset<Row> idAverage = sqlContext.sql(idAverageQuery);
		
		Double average = totalAverage.toJavaRDD().map(
			new Function<Row, Double>() {
				@Override
				public Double call(Row v) throws Exception {
					return v.getDouble(0);
				}
			}).collect().get(0);
		
			
		
		JavaPairRDD<String, Double> idAverageRDD = idAverage.toJavaRDD().mapToPair(
				new PairFunction<Row, String, Double>() {
					@Override
					public Tuple2<String, Double> call(Row v) throws Exception {
						return new Tuple2<String, Double>(v.getString(0), v.getDouble(1));
					}
				});
		
		//Calcola la distanza per ogni id
		JavaPairRDD<String, Double> idDistance= idAverageRDD.mapToPair(
				t -> new Tuple2<String, Double>(t._1, (Math.abs(average - t._2))));
		
		JavaPairRDD<String, Double> outputRDD = idDistance.mapToPair(
				t -> new Tuple2<Double, String>(t._2, t._1)).sortByKey(false).mapToPair(
				t -> new Tuple2<String, Double>(t._2, t._1));
		
		List<Tuple2<String, Double>> outputList = outputRDD.collect();
//		for (Tuple2<String, Double> tuple: outputList) {
//			System.out.println(tuple._1 + ": " + tuple._2);
//		}
		
		JavaRDD<Tuple2<String, Double>> output = ctx.parallelize(outputList, 1);
		output.saveAsTextFile(outputPath);
		
		ctx.stop();
		ctx.close();
	}

}
