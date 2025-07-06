English readme:
# Could They Meet?

##  Project Goal

This project determines whether two historical figures could have met during their lifetimes based on their birth and death dates.

---

## Project Structure

- **`Main.java`** – Handles user input, triggers index creation if necessary, and determines if the two figures could have met.
- **`CreateCSV.java`** – Parses an XML file with person data and converts it into a cleaned CSV format.
- **`Indexer.java`** – Creates a Lucene index from CSV data.
- **`ReadIndex.java`** – Searches the Lucene index for a specific person and retrieves their birth/death dates.

---

##  How It Works

### 1. Index Creation

If the Lucene index does **not** exist, it is created using:

```java
Indexer.createIndex()
```

This scans the designated folder for .csv files and builds an index from records containing:
name, date of birth, date of death

2. User Input and Comparison
The user is prompted to enter two names. For each name a search is performed in the Lucene index using ReadIndex.readIndex(name).
If found, the birth and death dates are retrieved. The program compares their lifespans to see if they overlap.

3. Output
If their lifespans overlap, program will print: "They could meet", otherwise: "They could not meet"

Data Preparation (CreateCSV.java)
It loads an XML file containing data about individuals. From this data, it extracts the name, birth date, and death date using regular expressions.
If the death date is missing, it is replaced with the current date. The dates are formatted into a single string (YYYY-MM-DD).
Records with invalid or null names are removed. The cleaned data is then saved as a CSV file.

Dependencies
Apache Lucene (for indexing and search functionality)
