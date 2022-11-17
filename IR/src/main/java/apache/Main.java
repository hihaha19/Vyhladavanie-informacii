package apache;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.*;

import org.apache.lucene.index.IndexWriter;


public class Main {

	public static Dataset<Row> removeColumns (Dataset<Row> dataset) {
		dataset = dataset.drop("Text", "Birth_year", "Birth_month", "Birth_day", "Death_year", "Death_month", "Death_day");
		
		return dataset;	
	}
	
	
	public static Dataset<Row> createNewColumns(Dataset<Row> namesRegex) {
		namesRegex = namesRegex.withColumn("Birth_date", concat(namesRegex.col("Birth_year"),lit('-'), namesRegex.col("Birth_month"),lit('-'),
				namesRegex.col("Birth_day")));
		
		namesRegex = namesRegex.withColumn("Death_date", concat(namesRegex.col("Death_year"),lit('-'), namesRegex.col("Death_month"),lit('-'),
				namesRegex.col("Death_day")));
		
		return namesRegex;
	}
	
	public static Dataset<Row> changeMonthToNumber(Dataset<Row> dataset) {	
		dataset = dataset.withColumn("Birth_month", 
				when(col("Birth_month").equalTo("January"), 1)
				  .when(col("Birth_month").equalTo("February"), 2)
				  .when(col("Birth_month").equalTo("March"), 3)
				  .when(col("Birth_month").equalTo("April"), 4)
				  .when(col("Birth_month").equalTo("May"), 5)
				  .when(col("Birth_month").equalTo("June"), 6)
				  .when(col("Birth_month").equalTo("July"), 7)
				  .when(col("Birth_month").equalTo("August"), 8)
				  .when(col("Birth_month").equalTo("September"), 9)
				  .when(col("Birth_month").equalTo("October"), 10)
				  .when(col("Birth_month").equalTo("November"), 11)
				  .when(col("Birth_month").equalTo("December"), 12)
				  .otherwise(null));
				
		dataset = dataset.withColumn("Death_month", 
						when(col("Death_month").equalTo("January"), 1)
						  .when(col("Death_month").equalTo("February"), 2)
						  .when(col("Death_month").equalTo("March"), 3)
						  .when(col("Death_month").equalTo("April"), 4)
						  .when(col("Death_month").equalTo("May"), 5)
						  .when(col("Death_month").equalTo("June"), 6)
						  .when(col("Death_month").equalTo("July"), 7)
						  .when(col("Death_month").equalTo("August"), 8)
						  .when(col("Death_month").equalTo("September"), 9)
						  .when(col("Death_month").equalTo("October"), 10)
						  .when(col("Death_month").equalTo("November"), 11)
						  .when(col("Death_month").equalTo("December"), 12)
						  .otherwise(null));
				
		return dataset;		
	}
	
	public static Dataset<Row> removeNotName (Dataset<Row> dataset) {
		dataset = dataset.withColumn("Name", when(col("Name").contains("| image"), null).otherwise(col("Name")));
		
		return dataset;	
	}
	

	
	public static void main(String[] args) throws Exception {
		
		BufferedReader reader = new BufferedReader(
	            new InputStreamReader(System.in));
	 
	        // Reading data using readLine
	 //   String name1 = reader.readLine();
	  //  String name2 = reader.readLine();
	//	Indexer.createIndex();
//		readIndex.readIndex();	//vrati 2 datumy
		
		
		SparkConf sparkConf = new SparkConf().setAppName("Mohli sa stretnut?").setMaster("spark://localhost:7077")
				.set("spark.executor.memory", "2G");
		SparkSession sparkSession=SparkSession.builder().config(sparkConf).getOrCreate();
		sparkSession.sparkContext().setLogLevel("ERROR");
		SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
		
		
		String filePath = "D:\\Stiahnuté súbory\\enwiki-latest-pages-articles.xml";
		System.out.println(filePath);
		System.out.println("Pred read");
		
		Dataset<Row> dataset=sparkSession.read()
				.option("rootTag", "page")
				.option("rowTag", "revision")
				.format("com.databricks.spark.xml")
				.load(filePath);

		System.out.println("Pred drop");
		dataset = dataset.drop("comment", "contributor", "format", "id", "minor", "model", "parentid", "sha1", "timestamp");
		dataset = dataset.withColumn("text", dataset.col("text").cast("string"));
		System.out.println("Pred filter");
		dataset
		.filter(dataset.col("text").rlike("(\\{\\{Infobox person)"));	//riadky s infobox person
		
		dataset.createOrReplaceTempView("cele_xml");
		
		Dataset<Row> infoboxDf = dataset.sqlContext().sql("SELECT * from cele_xml where rlike(text, '(\\\\{\\\\{Infobox person)')");

		infoboxDf.createOrReplaceTempView("infobox");
		String[] reorderedColumnNames = {"Text", "Name", "Birth_year", "Birth_month", "Birth_day", "Death_year", "Death_month", "Death_day"};
																			
		Dataset<Row> namesRegex1 = infoboxDf.sqlContext().sql("SELECT *, nullif(REGEXP_EXTRACT(text, '(?:name)\\\\s+[=]\\\\s(.*)', 1), '')  as Name,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 1), '')  as Birth_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 2), '') as Birth_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 3), '') as Birth_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 1), '') as Death_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 2), '') as Death_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 3), '') as Death_day FROM infobox");

		namesRegex1 = namesRegex1.na().drop();
		
		namesRegex1 = namesRegex1.select(reorderedColumnNames[0], Arrays.copyOfRange(reorderedColumnNames, 1, reorderedColumnNames.length));

		namesRegex1 = createNewColumns(namesRegex1);
		
		namesRegex1 = removeColumns(namesRegex1); 
		
		
		Dataset<Row> namesRegex2 = infoboxDf.sqlContext().sql("SELECT *,  nullif(REGEXP_EXTRACT(text, '(?:name)\\\\s+[=]\\\\s(.*)', 1), '')  as Name,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 1), '') as Birth_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 2), '') as Birth_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 3), '') as Birth_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 1), '') as Death_month, "
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 2), '') as Death_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 3), '') as Death_year FROM infobox");
		
		namesRegex2 = namesRegex2.na().drop();
		
		namesRegex2 = changeMonthToNumber(namesRegex2);
		
		namesRegex2 = namesRegex2.na().drop();	
		
		namesRegex2 = namesRegex2.select(reorderedColumnNames[0], Arrays.copyOfRange(reorderedColumnNames, 1, reorderedColumnNames.length));
		
		namesRegex2 = createNewColumns(namesRegex2);	
		namesRegex2 = removeColumns(namesRegex2); 

		
		Dataset<Row> namesRegex3 = infoboxDf.sqlContext().sql("SELECT *,  nullif(REGEXP_EXTRACT(text, '(?:name)\\\\s+[=]\\\\s(.*)', 1), '')  as Name,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 1), '') as Birth_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 2), '') as Birth_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 3), '') as Birth_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 1), '') as Death_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 2), '') as Death_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 3), '') as Death_year FROM infobox");
		
		
		namesRegex3 = namesRegex3.na().drop();	
		namesRegex3 = changeMonthToNumber(namesRegex3);
		
		namesRegex3 = removeNotName(namesRegex3);
		namesRegex3 = namesRegex3.na().drop();
		namesRegex3 = namesRegex3.select(reorderedColumnNames[0], Arrays.copyOfRange(reorderedColumnNames, 1, reorderedColumnNames.length));
	      
		
		namesRegex3 = createNewColumns(namesRegex3);
		namesRegex3 = removeColumns(namesRegex3); 


		Dataset<Row> mergedDataset = namesRegex1.union(namesRegex2).union(namesRegex3);
		System.out.println("Zapisujem do suboru");
		mergedDataset.write().option("header", false).option("delimiter", ", ").csv("velkaWiki");
		
	}
}

// id vsetkych ludi, index dal nad csv, kde mal ulozene meno, datumy
// pointa je parsovanie cez spark, asi aj search na klastri

// simple FS directory import asi uz nejde, datum indexovat ako int? 10042005 pred vybuildovanim query to dat na timestamp?
// mozeme povedat jednotlivym fieldom, ci ich chceme analyzovat