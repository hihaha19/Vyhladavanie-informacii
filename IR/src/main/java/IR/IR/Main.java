package version2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Main {	
	
	public static void main(String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(
	            new InputStreamReader(System.in));
	    
		String indexPath = "indexedFiles";
		Path indexFolder = Paths.get(indexPath);
		
		if (Files.notExists(indexFolder)) {
			System.out.println("I am creating index");
			Indexer.createIndex();	//creating an index over csv files
		}
	    	
	      
	   // Reading names from console using readLine
			System.out.println("Write first name");
		    String name1 = reader.readLine();
		    System.out.println("Write second name");
		    String name2 = reader.readLine();
	    
	    SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
		String[] argumentsName = ReadIndex.readIndex(name1);	//I will extract data about the first person, their name and dates from the index file
		
		if(argumentsName == null) {	//if it finds nothing, the program ends
			System.out.println("Person " + name1 + " is not in csv files");
			return;
		}
			
		
		if(!argumentsName[0].toLowerCase().equals(name1.toLowerCase())) {	// checking whether the person found is the same as the person sought
			System.out.println("Person " + name1 + " is not in csv files");
			return;
		}
				
		
		Date bd1 = CreateCSV.assignDateOfBirth(argumentsName, s);	// I assign the date of birth of the 1st person to the variable bd1
		Date dd1 = CreateCSV.assignDateOfDeath(argumentsName, s);	// I will assing the date of death to the variable dd1
		System.out.println(name1 + " " + s.format(bd1) + " " + s.format(dd1));	//printing dates of person 1
		
		// the same with the second person
		argumentsName = ReadIndex.readIndex(name2);
		
		if(argumentsName == null) {
			System.out.println("Person " + name2 + " is not in csv files");
			return;
		}
			
		
		if(!argumentsName[0].toLowerCase().equals(name2.toLowerCase())) {
			System.out.println("Person " + name2 + " is not in csv files");
			return;
		}
			
		Date bd2 = CreateCSV.assignDateOfBirth(argumentsName, s);
		Date dd2 = CreateCSV.assignDateOfDeath(argumentsName, s);
		
		System.out.println(name2 + " " + s.format(bd2) + " " + s.format(dd2));
		
		// if the dates are entered incorrectly and the person died earlier than they were born
		if(bd1.after(dd1)) {
			System.out.println("Person " + name1 + " died before birth");
			return;
		}
		
		if(bd2.after(dd2)) {
			System.out.println("Person " + name2 + " died before birth");
			return;
		}
		
		//  updated from https://www.codespeedy.com/check-if-two-date-ranges-overlap-or-not-in-java/
		if((bd1.before(bd2) || bd1.compareTo(bd2) == 0) && dd1.after(bd2) ||	// added OR because of the same date of birth
				(bd1.before(dd2) || bd1.compareTo(dd2) == 0) && (dd1.after(dd2) || dd1.compareTo(dd2) == 0) || //added OR because of the same date of birth and death
				bd1.before(bd2) && (dd1.after(dd2) || dd1.compareTo(bd2) == 0 ) ||
				bd1.after(bd2) && (dd1.before(dd2) || dd1.compareTo(dd2) == 0))	// because of the same date of death
			    {
			      System.out.print("They could meet");
			    }
		
		else System.out.println("They could not meet");
	
	}
}
