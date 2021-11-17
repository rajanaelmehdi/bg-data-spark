package org.aga.sparkinpractice.exercices;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aga.sparkinpractice.model.Sale;
import org.aga.sparkinpractice.model.Store;
import org.aga.sparkinpractice.model.TimeByDay;
import org.aga.sparkinpractice.utils.SparkUtil;
import org.aga.sparkinpractice.utils.Util;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MySparkDataSet {

	public static void main(String[] args) throws Exception {

		// Spark Session
		SparkSession sparkSession = SparkUtil.getSparkSession();

		// Exercie 1 - Spark Core 2

		Dataset<Row> df = sparkSession.read().format("csv").option("header", true).option("sep", ";")
				.load("../data/sales.csv");
		df.show();

		System.out.println("2========= Print Data ==============");
		df.show();

		// Exercie 1 - Spark Core 3

		Dataset<Row> dfs = sparkSession.read().format("csv").option("sep", ";").option("header", "true")
				.schema(Util.buildSchema(Sale.class)).load("../data/sales.csv");
		Encoder<Sale> saleEncoder = Encoders.bean(Sale.class);
		Dataset<Sale> as = dfs.as(saleEncoder);
		as.printSchema();
		System.out.println("3========= Print Data ==============");
		dfs.show();

		// Exercice 2 - Spark Core 4

		Dataset<Row> caByStore = dfs.select(col("storeId"), col("storeSales").multiply(col("unitSales")).as("rowCA"))
				.groupBy(col("storeId")).sum("rowCA").as("ca");
		caByStore.show();
		System.out.println("4========= Print Data ==============");
		caByStore.show();

		// Exercice 2 - Spark Core 5

		Dataset<Row> salesAsDF = sparkSession.read().format("csv").option("sep", ";").option("header", "false")
				.schema(Util.buildSchema(Sale.class)).load("../data/sales.csv");
		Dataset<Row> agg = salesAsDF.select(col("storeId"), col("unitSales")).groupBy(col("storeId"))
				.agg(count("unitSales"));
		List<Row> rows = agg.collectAsList();
		Map<Integer, Long> storeUnitSales = new HashMap<Integer, Long>();
		rows.stream().forEach(s -> storeUnitSales.put(s.getInt(0), s.getLong(1)));
		storeUnitSales.forEach((k, v) -> System.out.println("Nombre d'unités du magasin " + k + " est de " + v));

		
	}
}
