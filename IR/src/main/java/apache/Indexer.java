package apache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class Indexer {
	
	public static void createIndex() throws IOException {
		String docsPath = "NemazatWiki";	// priecinok s csv subormi 
		String indexPath = "indexedFiles";	// priecinok s indexom
		
		final java.nio.file.Path docDir = Paths.get(docsPath);
		Directory dir = FSDirectory.open(Paths.get(indexPath));
		
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);	
		IndexWriter writer = new IndexWriter(dir, iwc);	
		
		indexDocs(writer, docDir);
		
		writer.close();
		}
	
	static void indexDocs (final IndexWriter writer, java.nio.file.Path path) throws IOException {
		if(Files.isDirectory(path)) {
			Files.walkFileTree(path, new SimpleFileVisitor<java.nio.file.Path> () {
				public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) {
					try {
						if (file.toString().endsWith(".csv")) {	//kontrola, ci subor ktory chcem indexovat je csv subor
							BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
							String line = reader.readLine();	// idem po riadkoch
							while (line != null) {
								String splitedLine[] = line.split(",");
								String name = splitedLine[0];		//vytiahnem si meno osoby 
								String dateOfBirth = splitedLine[1];	// datum narodenia
								String dateOfDeath = splitedLine[2];	// datum umrtia 
								
								// vytvaranie indexu
								indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), name, dateOfBirth, dateOfDeath);
								line = reader.readLine();
							}
							
							reader.close();	
						}
						
					}
					catch (IOException ioe) {
						ioe.printStackTrace();
					}
					return FileVisitResult.CONTINUE;
				}
			});
		}
		else {
			//indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis(), null);
		}
	}
	
	static void indexDoc(IndexWriter writer, java.nio.file.Path file, long lastModified, String name, String dateOfBirth, String dateOfDeath) throws IOException {
		try (InputStream stream = Files.newInputStream(file)){
			Document doc = new Document();
			doc.add(new StringField("path", file.toString(), Field.Store.YES));	// idnexujem cestu k suboru
			doc.add(new TextField("name", name, Store.YES)); // meno
			doc.add(new TextField("dateOfBirth", dateOfBirth, Store.YES)); // datum narodenia
			doc.add(new TextField("dateOfDeath", dateOfDeath, Store.YES)); // datum umrtia
			writer.updateDocument(new Term("name", file.toString()), doc);
		}
	}
}
