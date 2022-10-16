package IR.IR;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

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
		
		File indexFile = new File(indexFilePath);
		
		ArrayList<String> riadky = XMLParser.getIndexData(dataFilePath);		
		String[] splitLine;
		FileWriter fw = new FileWriter(indexFile);
		 
		for (int i = 0; i < riadky.size(); i++) {
			
			splitLine = riadky.get(i).split(";");
			
			if(splitLine.length == 3 && isInt(splitLine[1]))	
			{								
				fw.write(riadky.get(i) + "\n");							
		}}
	 
		fw.close();
	}
	
	public HashMap<String, PostingList> loadIndex(String indexFilePath) throws IOException	
	{
		HashMap<String, PostingList> loadedIndex = new HashMap<String, PostingList>();
		
		File indexFile = new File(indexFilePath);
		
		String line;
		String[] splitLine;
		PostingList parsedLine;
		
		try(LineIterator it = FileUtils.lineIterator(indexFile, "UTF-8")) {
			while (it.hasNext()) {				
				  line = it.nextLine();
				  
				  System.out.println(line);
				  
				  splitLine = line.split(";");
				  parsedLine = new PostingList(Long.parseLong(splitLine[1]), splitLine[0], splitLine[2]);
				  loadedIndex.put(parsedLine.getPersonName(), parsedLine);				  
			}						
		}
		
		return loadedIndex;
	}
}	

