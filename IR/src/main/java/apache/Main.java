package apache;

import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;

public class Main {

	public static void main(String[] args) {
		SparkSession sparkSession=SparkSession.builder().appName("Read XML").master("local").getOrCreate();
		
		sparkSession.sparkContext().setLogLevel("ERROR");
		
		String filePath = "wiki_dump1.xml";
		System.out.println(filePath);
		
		Dataset<Row> dataset=sparkSession.read()
				.option("rootTag", "page")
				.option("rowTag", "revision")
				.format("com.databricks.spark.xml")
				.load(filePath);
		
	//	dataset.printSchema();
		
	//	dataset.show();
		
		/*dataset.write().option("rootTag", "page")
		.option("rowTag", "revision")
		.format("xml")
		.save("xmlFile");*/
		
		dataset = dataset.drop("comment", "contributor", "format", "id", "minor", "model", "parentid", "sha1", "timestamp");
		//dataset.printSchema();
		//dataset.show();

		dataset = dataset.withColumn("text", dataset.col("text").cast("string"));
				
		dataset
		.filter(dataset.col("text").rlike("(\\{\\{Infobox person)"));	//riadky s infobox person
		
		dataset.createOrReplaceTempView("cele_xml");
		
		Dataset<Row> infoboxDf = dataset.sqlContext().sql("SELECT * from cele_xml where rlike(text, '(\\\\{\\\\{Infobox person)')");
		
	//	df.write().format("com.databricks.spark.csv").save("textak");	//stranky s infoboxami
	//	df.write().option("header", true).option("delimiter", "\t").csv("f2.tsv");
		infoboxDf.createOrReplaceTempView("infobox");
		
																	
	//	infoboxDf.sqlContext().sql("SELECT *, REGEXP_EXTRACT(text, '(?:name)\\\\s+[=]\\\\s(.*)', 1) as MATCHED FROM infobox").printSchema();
		
		Dataset<Row> menaDf = infoboxDf.sqlContext().sql("SELECT *, REGEXP_EXTRACT(text, '(?:name)\\\\s+[=]\\\\s(.*)', 1) as Name, nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 1), '')  as Birth_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 2), '') as Birth_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 3), '') as Birth_day FROM infobox");
	//	menaDf = menaDf.withColumn("MATCHED", menaDf.col("MATCHED").cast("string"));
		//menaDf.select("Birth_year").write().text("newtextak2");
		
		menaDf.show();


		//	 menaDf.filter("Birth_year is NULL").show();
	}
}
// id vsetkych ludi, index dal nad csv, kde mal ulozene meno, datumy
// pointa je parsovanie cez spark, asi aj search na klastri

// simple FS directory import asi uz nejde, datum indexovat ako int? 10042005 pred vybuildovanim query to dat na timestamp?
// mozeme povedat jednotlivym fieldom, ci ich chceme analyzovat