package IR.IR;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	// teraz skusit submit cez spark
	
		public static void  main(String[] args) throws IOException, ParseException {		
			
			String dataFile = "wiki_dump1.xml-p1p41242"; 
			 String indexFile = "index.txt"; 
			 
			// BufferedReader txtReader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("index.txt")));
			 
			 SimpleDateFormat s=new SimpleDateFormat("yyyy-MM-dd");
			 Index index = new Index();			 
			// index.createIndex(dataFile, indexFile); // vytvorenie indexu - subor s menom, jeho poziciou v subore a nazvom suboru
			 
			 HashMap<String, PostingList> loadedIndex = index.loadIndex(indexFile); 
			 
			 File file = new File("input.txt");	// nacitam 2 osoby
			 BufferedReader br = new BufferedReader(new FileReader(file));			 
			 String line = br.readLine();
			 String[] splitLine = line.split(";");	// splitLine mam nacitane 2 osoby
			 br.close();
			 			 			 
		     Date bd1 = XMLParser.getBD(loadedIndex, splitLine[0], dataFile);	//datum narodenia 1.
		     Date dd1 = XMLParser.getDD(loadedIndex, splitLine[0], dataFile);	// datum umrtia 1.
		     Date bd2 = XMLParser.getBD(loadedIndex, splitLine[1], dataFile);	// datum narodenia 2.
		     Date dd2 = XMLParser.getDD(loadedIndex, splitLine[1], dataFile);

		     System.out.printf(splitLine[0] + " " + s.format(bd1) + " " + s.format(dd1) + "\n");
		     System.out.printf(splitLine[1] + " " + s.format(bd2) + " " + s.format(dd2) + "\n");		     	          
		     
		     // from https://www.codespeedy.com/check-if-two-date-ranges-overlap-or-not-in-java/
				if(bd1.before(bd2) && dd1.after(bd2) ||
						bd1.before(dd2) && dd1.after(dd2) ||
						bd1.before(bd2) && dd1.after(dd2) ||
						bd1.after(bd2) && dd1.before(dd2) )
					    {
					      System.out.print("Mohli sa stretnut");
					    }
				
				else System.out.println("Nemohli sa stretnut");
		     
	}
		
}
