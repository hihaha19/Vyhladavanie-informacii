package version2;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateCSV {
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
		dataset = dataset.withColumn("Name", when(col("Name").contains("<!--"), null).otherwise(col("Name")));
		dataset = dataset.withColumn("Name", when(col("Name").contains("| birth_date"), null).otherwise(col("Name")));
		return dataset;	
	}
	
	public static Date assignDateOfBirth(String[] arguments, SimpleDateFormat s) throws ParseException {		
		String date = arguments[1];
		Date dateOfBirth = s.parse(date);		
		return dateOfBirth;		
	}
	
	public static Date assignDateOfDeath(String[] arguments, SimpleDateFormat s) throws ParseException {		
		String date = arguments[2];
		Date dateOfDeath = s.parse(date);		
		return dateOfDeath;		
	}

	public static void createCSV(String filePath) {
		SparkConf sparkConf = new SparkConf().setAppName("Mohli sa stretnut?").setMaster("spark://localhost:7077")
				.set("spark.executor.memory", "4G");
		
		SparkSession sparkSession=SparkSession.builder().config(sparkConf).getOrCreate();
		sparkSession.sparkContext().setLogLevel("ERROR");		
		
		LocalDate d = LocalDate.now();
		
		//the path to the file I want to analyze
		System.out.println(filePath);
		
		Dataset<Row> dataset=sparkSession.read()	//I will create a dataset in which I will have infoboxes about persons
				.option("rootTag", "page")
				.option("rowTag", "revision")
				.format("com.databricks.spark.xml")
				.load(filePath);

		dataset = dataset.drop("comment", "contributor", "format", "id", "minor", "model", "parentid", "sha1", "timestamp");	//I delete unnecessary columns from the dataset
		dataset = dataset.withColumn("text", dataset.col("text").cast("string"));
		dataset.filter(dataset.col("text").rlike("(\\{\\{Infobox person)"));	//hladam v stlpci riadky s infobox person
		dataset.createOrReplaceTempView("cele_xml");	
		
		Dataset<Row> infoboxDf = dataset.sqlContext().sql("SELECT * from cele_xml where rlike(text, '(\\\\{\\\\{Infobox person)')");
		infoboxDf.createOrReplaceTempView("infobox");
		
		String[] reorderedColumnNames = {"Text", "Name", "Birth_year", "Birth_month", "Birth_day", "Death_year", "Death_month", "Death_day"};	
		
		// I select birth year, month, day, death year, month, day using regex
		Dataset<Row> namesRegex1 = infoboxDf.sqlContext().sql("SELECT *, nullif(REGEXP_EXTRACT(text, '(?:name)\\\\s+[=]\\\\s(.*)', 1), '')  as Name,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 1), '')  as Birth_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 2), '') as Birth_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 3), '') as Birth_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 1), '') as Death_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 2), '') as Death_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 3), '') as Death_day FROM infobox");
		
		// if it does not find the date of death, it replaces this date with today's date
		namesRegex1 = namesRegex1.withColumn("Death_day", when(col("Death_day").isNull(), d.getDayOfMonth()).otherwise(col("Death_day")));
		namesRegex1 = namesRegex1.withColumn("Death_month", when(col("Death_month").isNull(), d.getMonthValue()).otherwise(col("Death_month")));
		namesRegex1 = namesRegex1.withColumn("Death_year", when(col("Death_year").isNull(), d.getYear()).otherwise(col("Death_year")));
		
		namesRegex1 = namesRegex1.na().drop();	// drop rows with null
		namesRegex1 = namesRegex1.select(reorderedColumnNames[0], Arrays.copyOfRange(reorderedColumnNames, 1, reorderedColumnNames.length));	//reorder columns
		namesRegex1 = createNewColumns(namesRegex1);	// from 3 columns (Birth_year, Birth_month, Birth_day) create 1 column - Birth_date
		namesRegex1 = removeColumns(namesRegex1); 	// drop all columns from dataset except Name, Birth_date, Death_date
		
		
		// the same with another regex (month (in word), day, year)
		Dataset<Row> namesRegex2 = infoboxDf.sqlContext().sql("SELECT *,  nullif(REGEXP_EXTRACT(text, '(?:name)\\\\s+[=]\\\\s(.*)', 1), '')  as Name,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 1), '') as Birth_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 2), '') as Birth_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 3), '') as Birth_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 1), '') as Death_month, "
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 2), '') as Death_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)\\\\s+[=]\\\\s+([a-zA-Z]*)\\\\s+(\\\\d+)[^0-9]*(\\\\d+)', 3), '') as Death_year FROM infobox");
		
		namesRegex2 = namesRegex2.withColumn("Death_day", when(col("Death_day").isNull(), d.getDayOfMonth()).otherwise(col("Death_day")));
		namesRegex2 = namesRegex2.withColumn("Death_month", when(col("Death_month").isNull(), d.getMonthValue()).otherwise(col("Death_month")));
		namesRegex2 = namesRegex2.withColumn("Death_year", when(col("Death_year").isNull(), d.getYear()).otherwise(col("Death_year")));

		
		
		namesRegex2 = namesRegex2.na().drop();
		namesRegex2 = changeMonthToNumber(namesRegex2);	// change month from word to number
		namesRegex2 = namesRegex2.select(reorderedColumnNames[0], Arrays.copyOfRange(reorderedColumnNames, 1, reorderedColumnNames.length));
		
		namesRegex2 = createNewColumns(namesRegex2);	
		namesRegex2 = removeColumns(namesRegex2); 

		// the same with third regex (day, month (in word), year)
		Dataset<Row> namesRegex3 = infoboxDf.sqlContext().sql("SELECT *,  nullif(REGEXP_EXTRACT(text, '(?:name)\\\\s+[=]\\\\s(.*)', 1), '')  as Name,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 1), '') as Birth_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 2), '') as Birth_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 3), '') as Birth_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 1), '') as Death_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 2), '') as Death_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d+)\\\\s([a-zA-Z]*)\\\\s(\\\\d+)', 3), '') as Death_year FROM infobox");
		

		namesRegex3 = namesRegex3.withColumn("Death_day", when(col("Death_day").isNull(), d.getDayOfMonth()).otherwise(col("Death_day")));
		namesRegex3 = namesRegex3.withColumn("Death_month", when(col("Death_month").isNull(), d.getMonthValue()).otherwise(col("Death_month")));
		namesRegex3 = namesRegex3.withColumn("Death_year", when(col("Death_year").isNull(), d.getYear()).otherwise(col("Death_year")));

		namesRegex3 = namesRegex3.na().drop();	
		namesRegex3 = changeMonthToNumber(namesRegex3);
		
		namesRegex3 = namesRegex3.select(reorderedColumnNames[0], Arrays.copyOfRange(reorderedColumnNames, 1, reorderedColumnNames.length));
		namesRegex3 = createNewColumns(namesRegex3);
		namesRegex3 = removeColumns(namesRegex3); 

		
		Dataset<Row> mergedDataset = namesRegex1.union(namesRegex2).union(namesRegex3);	//I merge 3 datasets together
		mergedDataset = removeNotName(mergedDataset);	// Remove rows with "<!--" or "| image" in column name
		mergedDataset = mergedDataset.na().drop();	// drop rows with Null value
		mergedDataset.write().option("header", false).option("delimiter", ", ").csv("D:\\wikiCSV");		// save merged dataset into csv files
		mergedDataset.show();
	}
}
