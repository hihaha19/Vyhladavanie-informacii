package version1;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Scanner;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Date;
import java.util.HashMap;



public class XMLParser {
	
	
	
	private static Date findBirthday(String multiLines) throws ParseException {
		Date dateOfBirth = null;
		SimpleDateFormat s=new SimpleDateFormat("yyyy-MM-dd");
		Pattern pNarodenie = Pattern.compile("birth_date[^0-9]*(\\d{4})\\|(\\d+)\\|(\\d+)");
		  Matcher mNarodenie = pNarodenie.matcher(multiLines);
		  int mamNarodenie = 0;
			if(mNarodenie.find()) {
					String rokNarodenia = mNarodenie.group(1);
		            String mesiacNarodenia = mNarodenie.group(2);
		            String denNarodenia = mNarodenie.group(3);
		            mamNarodenie = 1;
		            dateOfBirth=s.parse(rokNarodenia + "-" + mesiacNarodenia + "-" + denNarodenia);
		        //    System.out.println("Datum narodenia 1. osoby " + s.format(dateOfBirth));        
				}
			
		if(mamNarodenie == 0) {
          	Pattern pNarodenieSlovo = Pattern.compile("birth_date\\s+[=]\\s+([a-zA-Z]*)\\s+(\\d+)[^0-9]*(\\d+)");
          	mNarodenie = pNarodenieSlovo.matcher(multiLines);
          	
          	while (mNarodenie.find()) {								          	            
			            String birthMonth = mNarodenie.group(1);
			            String birthDay = mNarodenie.group(2);
			            String birthYear = mNarodenie.group(3);
			            mamNarodenie = 1;
			            Date date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(birthMonth);
			            Calendar cal = Calendar.getInstance();
			            cal.setTime(date);
			            int month = cal.get(Calendar.MONTH);
			            month = month + 1;
			            dateOfBirth=s.parse(birthYear + "-" + month + "-" + birthDay);	
			         //   System.out.println("Datum narodenia 1. osoby" + s.format(dateOfBirth));
          	}				            	
          }
		
		if(mamNarodenie == 0) {		
			 Pattern pNarodenieSlovo = Pattern.compile("birth_date[^0-9]*(\\d+)\\s([a-zA-Z]*)\\s(\\d+)");
          	mNarodenie = pNarodenieSlovo.matcher(multiLines);
          	while (mNarodenie.find()) {		
          		String birthDay = mNarodenie.group(1);
          		String birthMonth = mNarodenie.group(2);
          		 String birthYear = mNarodenie.group(3);
          		 mamNarodenie = 1;
          		
          		 Date date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(birthMonth);
		            Calendar cal = Calendar.getInstance();
		            cal.setTime(date);
		            int month = cal.get(Calendar.MONTH);
		            month = month + 1;
		            dateOfBirth=s.parse(birthYear + "-" + month + "-" + birthDay);			
		           // System.out.println("Datum narodenia 1 " + s.format(datumNarodenia1));
          	}
		  }
		
		return dateOfBirth;	
	}
	
	private static Date findDeath(String multiLines) throws ParseException {
		Date dateOfDeath = null;
		SimpleDateFormat s=new SimpleDateFormat("yyyy-MM-dd");
		Date todayDate = new Date();
		
		 Pattern pUmrtie = Pattern.compile("death_date[^0-9]*(\\d{4})\\|(\\d+)\\|(\\d+)");
		 Matcher mUmrtie = pUmrtie.matcher(multiLines);
		 int haveDeath = 0;
		 if(mUmrtie.find()) {	
	            String rokUmrtia = mUmrtie.group(1);
	            String mesiacUmrtia = mUmrtie.group(2);
	            String denUmrtia = mUmrtie.group(3);
	            haveDeath = 1;
	            dateOfDeath =s.parse(rokUmrtia + "-" + mesiacUmrtia + "-" + denUmrtia);  
	          //  System.out.println("Datum umrtia 1. osoby " + s.format(dateOfDeath));								 
		 }
		 
			if(haveDeath == 0) {
	            	Pattern pUmrtieSlovo = Pattern.compile("death_date\\s+[=]\\s+([a-zA-Z]*)\\s+(\\d+)[^0-9]*(\\d+)");
	            	mUmrtie = pUmrtieSlovo.matcher(multiLines);
	            	
	            	while (mUmrtie.find()) {								          	            
				            String mesiacNarodenia = mUmrtie.group(1);
				            String denNarodenia = mUmrtie.group(2);
				            String rokNarodenia = mUmrtie.group(3);
				            
				            Date date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(mesiacNarodenia);
				            Calendar cal = Calendar.getInstance();
				            cal.setTime(date);
				            int month = cal.get(Calendar.MONTH);
				            month = month + 1;
				            dateOfDeath=s.parse(rokNarodenia + "-" + month + "-" + denNarodenia);	
				         //   System.out.println("Datum umrtia 1. osoby " + s.format(dateOfDeath));
	            	}				            	
	            }
		 
		 if(haveDeath == 0) {
			 Pattern pUmrtieSlovo = Pattern.compile("death_date[^0-9]*(\\d+)\\s([a-zA-Z]*)\\s(\\d+)");
           	mUmrtie = pUmrtieSlovo.matcher(multiLines);
           	
           	while (mUmrtie.find()) {					            		
			            String mesiacUmrtia = mUmrtie.group(2);
			            String denUmrtia = mUmrtie.group(1);
			            String rokUmrtia = mUmrtie.group(3);
			            
			            haveDeath = 1;
			            Date date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(mesiacUmrtia);
			            Calendar cal = Calendar.getInstance();
			            cal.setTime(date);
			            int month = cal.get(Calendar.MONTH);
			            month = month + 1;
			            dateOfDeath=s.parse(rokUmrtia + "-" + month + "-" + denUmrtia);	
			            System.out.println("Umrtie 2. osoby" + s.format(dateOfDeath));
           	}
		 }
		 
		 if(haveDeath == 0) {
			 dateOfDeath = todayDate;
		 }
		
		return dateOfDeath;
	}
	
	
	private static String getMultilines(String dataFilePath, long position) throws IOException
	{
		File f=new File(dataFilePath);     
		RandomAccessFile rf = new RandomAccessFile(f, "r");
		rf.seek(position);	// nastavim sa na poziciu 
		
		int i=0;
		String multilines = ""; 
		
		while(i++ < 12)
			multilines += rf.readLine() + "\n";
		

		return multilines;
	}
	
	public static Date getBD(HashMap<String, PostingList> index, String name, String dataFilePath) throws ParseException, IOException
	{
		PostingList pl = index.get(name);
		String ml = getMultilines(dataFilePath, pl.getPosition());
		return findBirthday(ml);
	}
	
	public static Date getDD(HashMap<String, PostingList> index, String name, String dataFilePath) throws ParseException, IOException
	{
		PostingList pl = index.get(name);
		String ml = getMultilines(dataFilePath, pl.getPosition());
		return findDeath(ml);
	}
	
	
	public static ArrayList<String> getIndexData(String dataFilePath) throws IOException {	
		
		File dataFile = new File(dataFilePath);
		RandomAccessFile rf = new RandomAccessFile(dataFile, "r");
		ArrayList<String> indexLines = new ArrayList<String>();
		Pattern infobox = Pattern.compile("(\\{\\{Infobox person)");		
		String line = null;
		line = rf.readLine();
			
		while(line != null) {	//citam cely xml subor
			     
			    String multiLines = null;
			    Pattern person = Pattern.compile("(?:name)\\s+[=]\\s(.*)");
			    
			    Matcher infoPerson = infobox.matcher(line);
			    boolean matchFound = infoPerson.find();			// hladam infobox

			    Long position = rf.getFilePointer();			// mam ulozenu aktualnu poziciu v subore
			    
			    if(matchFound) {						//ak najdem infobox
					for (int i = 0; i <= 15; i++)		// ulozim si 16 dalsich riadkov
			        {
						line = rf.readLine();
			            if (multiLines == null)
			            	multiLines = line + "\n";
			            
			            else {
			            	multiLines = multiLines + line + "\n";    
			            }      	
			        }
					
					Matcher matcher = person.matcher(multiLines);	//ak v 16tich riadkoch najdem nejake meno
				    if(matcher.find()) {
				    	indexLines.add(matcher.group(1)+";"+(position)+";"+dataFile.getName());		// pridam do index lines meno osoby, poziciu v subore a nazov suboru
			            System.out.println(matcher.group(1)); 

	
			    
			    System.out.println(position);
			    
			  }
			  }line = rf.readLine();}
		return indexLines;
	}

}
