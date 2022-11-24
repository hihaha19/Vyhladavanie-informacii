package version1;

public class PostingList {
	
	private long position;
	private String personName;
	private String documentPath;
	
	public PostingList(long position, String personName, String documentPath)
	{
		this.position = position;
		this.personName = personName;
		this.documentPath = documentPath;
	}
	
	public long getPosition()
	{
		return this.position;
	}
	
	public String getPersonName()
	{
		return this.personName;
	}
	
	public String getDocumentPath()
	{
		return this.documentPath;
	}

}
