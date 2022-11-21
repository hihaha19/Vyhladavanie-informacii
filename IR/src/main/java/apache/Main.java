package apache;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.time.LocalDate;
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
	
	public static Date assignDateOfBirth(String[] arguments, SimpleDateFormat s) throws ParseException {		
		String date = arguments[1];
		Date dateOfBirth = s.parse(date);
		//System.out.println("Datum narodenia " + s.format(dateOfBirth));
		
		return dateOfBirth;		
	}
	
	public static Date assignDateOfDeath(String[] arguments, SimpleDateFormat s) throws ParseException {		
		String date = arguments[2];
		Date dateOfDeath = s.parse(date);
	//	System.out.println("Datum umrtia " + s.format(dateOfDeath));
		
		return dateOfDeath;		
	}

	public static void createCSV() {
		SparkConf sparkConf = new SparkConf().setAppName("Mohli sa stretnut?").setMaster("spark://localhost:7077")
				.set("spark.executor.memory", "3G");
		
		SparkSession sparkSession=SparkSession.builder().config(sparkConf).getOrCreate();
		sparkSession.sparkContext().setLogLevel("ERROR");		
		LocalDate d = LocalDate.now();
		
		String filePath = "C:\\enwiki-latest-pages-articles.xml";	//cesta k suboru, ktory chcem analyzovat 
		System.out.println(filePath);
		
		Dataset<Row> dataset=sparkSession.read()	//vytvorim si dataset, v ktorom budem mať infoboxy o osobach 
				.option("rootTag", "page")
				.option("rowTag", "revision")
				.format("com.databricks.spark.xml")
				.load(filePath);

		dataset = dataset.drop("comment", "contributor", "format", "id", "minor", "model", "parentid", "sha1", "timestamp");	// vymazem nepotrebne stlpce z datasetu
		dataset = dataset.withColumn("text", dataset.col("text").cast("string"));
		dataset.filter(dataset.col("text").rlike("(\\{\\{Infobox person)"));	//hladam v stlpci riadky s infobox person
		dataset.createOrReplaceTempView("cele_xml");	
		
		Dataset<Row> infoboxDf = dataset.sqlContext().sql("SELECT * from cele_xml where rlike(text, '(\\\\{\\\\{Infobox person)')");
		infoboxDf.createOrReplaceTempView("infobox");
		
		String[] reorderedColumnNames = {"Text", "Name", "Birth_year", "Birth_month", "Birth_day", "Death_year", "Death_month", "Death_day"};	
		
		// vyberiem pomocou regexu rok narodenia, mesiac, den, rok umrtia, mesiac, den
		Dataset<Row> namesRegex1 = infoboxDf.sqlContext().sql("SELECT *, nullif(REGEXP_EXTRACT(text, '(?:name)\\\\s+[=]\\\\s(.*)', 1), '')  as Name,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 1), '')  as Birth_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 2), '') as Birth_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:birth_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 3), '') as Birth_day,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 1), '') as Death_year,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 2), '') as Death_month,"
				+ "nullif(REGEXP_EXTRACT(text, '(?:death_date)[^0-9]*(\\\\d{4})\\\\|(\\\\d+)\\\\|(\\\\d+)', 3), '') as Death_day FROM infobox");
		
		// ak nenajde v regexe nejaky udaj, nahrad tento udaj s null
		namesRegex1 = namesRegex1.withColumn("Death_day", when(col("Death_day").isNull(), d.getDayOfMonth()).otherwise(col("Death_day")));
		namesRegex1 = namesRegex1.withColumn("Death_month", when(col("Death_month").isNull(), d.getMonthValue()).otherwise(col("Death_month")));
		namesRegex1 = namesRegex1.withColumn("Death_year", when(col("Death_year").isNull(), d.getYear()).otherwise(col("Death_year")));
		namesRegex1.show();
		
		namesRegex1 = namesRegex1.na().drop();	// vyhod riadky s hodnotou null
		namesRegex1 = namesRegex1.select(reorderedColumnNames[0], Arrays.copyOfRange(reorderedColumnNames, 1, reorderedColumnNames.length));
		namesRegex1 = createNewColumns(namesRegex1);	// z 3 stlpcov: rok narodenia, mesiac a den vytvor 1 - datum narodenia
		namesRegex1 = removeColumns(namesRegex1); 	// vyhod z datasetu nepotrebne stlpce (vsetky okrem mena, datumu narodenia a umrtia)
		
		
		// to iste s inym regexom (mesiac, den, rok)
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
		namesRegex2 = changeMonthToNumber(namesRegex2);
		
		namesRegex2 = namesRegex2.na().drop();	
		namesRegex2 = namesRegex2.select(reorderedColumnNames[0], Arrays.copyOfRange(reorderedColumnNames, 1, reorderedColumnNames.length));
		
		namesRegex2 = createNewColumns(namesRegex2);	
		namesRegex2 = removeColumns(namesRegex2); 

		// to iste s 3. typom regexu (den, mesiac, rok)
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
		
		namesRegex3 = removeNotName(namesRegex3);
		namesRegex3 = namesRegex3.na().drop();
		namesRegex3 = namesRegex3.select(reorderedColumnNames[0], Arrays.copyOfRange(reorderedColumnNames, 1, reorderedColumnNames.length));
	      
		
		namesRegex3 = createNewColumns(namesRegex3);
		namesRegex3 = removeColumns(namesRegex3); 

		//spojim 3 datasety a ulozim ich do csv suborov
		Dataset<Row> mergedDataset = namesRegex1.union(namesRegex2).union(namesRegex3);
		mergedDataset.write().option("header", false).option("delimiter", ", ").csv("D:\\wikiCSV");
		mergedDataset.show();
		
	}
	
	
	public static void main(String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(
	            new InputStreamReader(System.in));
	 
	    // Reading data using readLine
	    String name1 = reader.readLine();
	    String name2 = reader.readLine();
	//	Indexer.createIndex();	//vytvorenie indexu nad csv subormi
	    SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
		String[] argumentsName = readIndex.readIndex(name1);	//vytiahnem zo suboru s indexmi udaje o prvej osobe, jej meno a datumy
		
		if(argumentsName == null)	//ak nenajde nic, program skonci
			return;
		
		if(!argumentsName[0].toLowerCase().equals(name1.toLowerCase())) {	// kontrola, ci sa najdena osoba rovna hladanej osobe
			System.out.println("Osoba " + name1 + " is not in file");
			return;
		}
				
		
		Date bd1 = assignDateOfBirth(argumentsName, s);	// do premennej bd1 priradim datum narodenia 1. osoby
		Date dd1 = assignDateOfDeath(argumentsName, s);	// do premennej dd1 pridadim datum umrtia
		System.out.println(name1 + " " + s.format(bd1) + " " + s.format(dd1));
		
		// to iste s druhou osobou
		argumentsName = readIndex.readIndex(name2);
		
		if(argumentsName == null)
			return;
		
		if(!argumentsName[0].toLowerCase().equals(name2.toLowerCase())) {
			System.out.println("Osoba " + name2 + " is not in file");
			return;
		}
			
		
		Date bd2 = assignDateOfBirth(argumentsName, s);
		Date dd2 = assignDateOfDeath(argumentsName, s);
		System.out.println(name2 + " " + s.format(bd2) + " " + s.format(dd2));
		
		//ak su zle zadane datumy a osoba umrela skor ako sa narodila
		if(bd1.after(dd1)) {
			System.out.println("Person " + name1 + " died before birth");
			return;
		}
		
		if(bd2.after(dd2)) {
			System.out.println("Person " + name2 + " died before birth");
			return;
		}
		
		// hladanie prienikov datumov
		if((bd1.before(bd2) || bd1.compareTo(bd2) == 0) && dd1.after(bd2) ||	// because of the same date of birth
				(bd1.before(dd2) || bd1.compareTo(dd2) == 0) && (dd1.after(dd2) || dd1.compareTo(dd2) == 0) || //because of the same date of birth and death
				bd1.before(bd2) && (dd1.after(dd2) || dd1.compareTo(bd2) == 0 ) ||
				bd1.after(bd2) && (dd1.before(dd2) || dd1.compareTo(dd2) == 0))	// because of the same date of death
			    {
			      System.out.print("Mohli sa stretnut");
			    }
		
		else System.out.println("Nemohli sa stretnut");
	
	}
}
