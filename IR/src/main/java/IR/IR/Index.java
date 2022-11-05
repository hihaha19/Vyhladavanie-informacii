package IR.IR;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.spark.api.java.JavaRDD;

public class Index {
	
	private boolean isInt(String value) {
	    try {
	         Long.parseLong(value);
	    } catch (NumberFormatException e) {
	        return false;
	    }
	    return true;
	}
	
	public void createIndex(String dataFilePath, String indexFilePath) throws IOException {			
		
		File indexFile = new File(indexFilePath);	// indexFile je subor do ktoreho sa ulozia indexy
		
		ArrayList<String> riadky = XMLParser.getIndexData(dataFilePath);		// v riadkoch mam meno osoby, jej poziciu a nazov suboru
		String[] splitLine;
		FileWriter fw = new FileWriter(indexFile);
		 
		for (int i = 0; i < riadky.size(); i++) {
			
			splitLine = riadky.get(i).split(";");	// prechadzam "indexy" po jednom
			
			if(splitLine.length == 3 && isInt(splitLine[1]))		//ak maju dlzku 3 a na 2. mieste je int (pozicia v subore)
			{								
				fw.write(riadky.get(i) + "\n");		// zapisem index do suboru index					
		}}
	 
		fw.close();
	}
	
	public HashMap<String, PostingList> loadIndex(String indexSubor) throws IOException	
	{
		HashMap<String, PostingList> loadedIndex = new HashMap<String, PostingList>();	//vytvorim hash mapu s jednym zaznamom
		
		File indexFile = new File(indexSubor);
		String line;
		String[] splitLine;
		PostingList parsedLine;
		
		try(LineIterator it = FileUtils.lineIterator(indexFile, "UTF-8")) {
			while (it.hasNext()) {					
				  line = it.nextLine();
				  
				//  System.out.println(line);
				  
				  splitLine = line.split(";");
				  parsedLine = new PostingList(Long.parseLong(splitLine[1]), splitLine[0], splitLine[2]); //vytvorim instanciu Posting listu
				  loadedIndex.put(parsedLine.getPersonName(), parsedLine);		// vytvori kluc osoba s hodnotou parsed Line	
			}						
		}
		
		return loadedIndex;
	}
}	

