package version2;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class ReadIndex {
	private static final String INDEX_DIR = "indexedFiles";
	
	public static String[] readIndex(String name) throws Exception {
		IndexSearcher searcher = createSearcher();	// creating searcher
		
		TopDocs foundDocs = searchInContent(name, searcher);
		
		for (ScoreDoc sd : foundDocs.scoreDocs) {
			Document d = searcher.doc(sd.doc);	// from this file I extract name and dates
			return new String[] { d.get("name"), d.get("dateOfBirth"), d.get("dateOfDeath")};
		}
		return null; 
		
	} 
	
	private static TopDocs searchInContent(String textToFind, IndexSearcher searcher) throws Exception {
		QueryParser qp = new QueryParser("name", new StandardAnalyzer());	// find name in index
		Query query = qp.parse(textToFind);		

		
		TopDocs hits = searcher.search(query, 1);
		return hits;		
	}

	private static IndexSearcher createSearcher() throws IOException {
		Directory dir = FSDirectory.open(Paths.get(INDEX_DIR));
		
		IndexReader reader = DirectoryReader.open(dir);
		
		IndexSearcher searcher = new IndexSearcher(reader);
		return searcher;
	}
} 
