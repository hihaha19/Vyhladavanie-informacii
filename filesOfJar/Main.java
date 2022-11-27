package version2;


public class Main {	
	
	public static void main(String[] args) throws Exception {
		String filePathOfWikidump = "C:\\enwiki-latest-pages-articles.xml";	// the path to the file I want to index

	    
	    CreateCSV.createCSV(filePathOfWikidump);	//creating csv files from xml file
	
	}
}
