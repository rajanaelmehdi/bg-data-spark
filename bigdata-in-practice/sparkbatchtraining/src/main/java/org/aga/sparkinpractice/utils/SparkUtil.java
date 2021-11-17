package org.aga.sparkinpractice.utils;

import org.apache.spark.sql.SparkSession;

public class SparkUtil {
	
	public static SparkSession getSparkSession() {
		SparkSession spark = SparkSession.builder()
				.master("local[*]")
	            .appName("spark-bigquery-demo")
	            .getOrCreate();
		return spark;
	}

}
