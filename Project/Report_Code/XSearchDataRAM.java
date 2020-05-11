import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.TokenStream.*;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.store.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import org.apache.lucene.document.*;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import java.io.*;
import java.util.*;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.w3c.dom.Element;

class MyThread extends Thread {
 ArrayList < Document > docs;
 IndexWriter iwriter;
 int tId;
 int numThreads;
 public MyThread(ArrayList < Document > docs, IndexWriter iwriter, int tId, int numThreads) {
  this.docs = docs;
  this.iwriter = iwriter;
  this.tId = tId;
  this.numThreads = numThreads;
 }
 public void run() {
  //inside run()
  try {


   for (int x = tId; x < docs.size(); x += numThreads) {
    iwriter.addDocument(docs.get(x));

   }

  } catch (Exception e) {
   System.out.println("Exception in run()" + e);
  }
 }
}
abstract class XSearchData extends Analyzer {
 public static void main(String[] args) {
  Analyzer analyzer;
  RAMDirectory directory;
  IndexWriterConfig config;
  IndexWriter iwriter;
  DirectoryReader ireader;
  IndexSearcher isearcher;
  QueryBuilder builder;
  RandomAccessFile in ;
  ArrayList < String > inputFiles;
  String termsFile;
  ArrayList < Document > documents;
  String line;
  long start, end;
  long indexTime, indexSize, searchTime;
  int termsCount = 0;
  double latency = 0 L;
  double throughput = 0 L;
  double filesLength = 0 L;
  int numDocs = 0;
  indexTime = 0;
  indexSize = 0;
  searchTime = 0;
  try {
   // read the file paths from the input file
   inputFiles = new ArrayList < String > (); in = new RandomAccessFile(args[0], "r");
   while ((line = in .readLine()) != null) {
    inputFiles.add(line);
   } in .close();

   // create a list of document that are going to be indexed
   documents = new ArrayList < Document > ();
   for (String inputFile: inputFiles) {
    File file = new File(inputFile);
    filesLength += file.length();
    Document document = new Document();

    Field contentField = new Field("content", new InputStreamReader(new FileInputStream(file)),
     TextField.TYPE_NOT_STORED);
    Field filenameField = new Field("filename", file.getName(), StoredField.TYPE);
    Field filepathField = new Field("filepath", file.getCanonicalPath(), StoredField.TYPE);

    document.add(contentField);
    document.add(filenameField);
    document.add(filepathField);

    documents.add(document);
   }

   // use the standard text analyzer
   analyzer = new StandardAnalyzer();



   // store the index in main memory (RAM)
   directory = new RAMDirectory();

   // create and index writer
   config = new IndexWriterConfig(analyzer);
   config.setUseCompoundFile(false);

   //

   double ramBufferSizeMB = IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB;
   System.out.println("Default RAM BufferSize (MB):" + ramBufferSizeMB);
   //config.setMaxBufferedDocs(100000);
   //System.out.println("RAM-"+config.getRAMBufferSizeMB());
   config.setRAMBufferSizeMB(determineGoodBufferSize(config.getRAMBufferSizeMB()));
   System.out.println("RAM BufferSize After(MB):" + config.getRAMBufferSizeMB());
   ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) config.getMergeScheduler();
   //int maxThreadCount = config.get("concurrent.merge.scheduler.max.thread.count");
   //int maxMergeCount = config.get("concurrent.merge.scheduler.max.merge.count");

   cms.setMaxMergesAndThreads(4, 4);
   iwriter = new IndexWriter(directory, config.setMergeScheduler(cms));

   // index each file one by one and measure the time taken
   start = System.currentTimeMillis();
   int numThreads = 4;
   MyThread mythread[] = new MyThread[numThreads];

   try {
    for (int x = 0; x < numThreads; x++) {
     mythread[x] = new MyThread(documents, iwriter, x, numThreads);
     mythread[x].start();
    }

    for (int x = 0; x < numThreads; x++) {
     mythread[x].join();
    }
   } catch (Exception e) {
    System.out.println(e);
   }
   iwriter.commit();
   iwriter.close();


   end = System.currentTimeMillis();

   // calculate the time taken to index the files
   indexTime = (end - start);

   // get the total size of the index
   indexSize = directory.ramBytesUsed() / 1000;

   // create an index reader
   ireader = DirectoryReader.open(directory);
   isearcher = new IndexSearcher(ireader);
   builder = new QueryBuilder(analyzer);
   numDocs = ireader.numDocs();
   //System.out.println("Docs"+ireader.numDocs());
   // read the terms from the second input file and search the index
   in = new RandomAccessFile(args[1], "r");

   start = System.currentTimeMillis();

   while ((line = in .readLine()) != null) {

    StringTokenizer st = new StringTokenizer(line);
    termsCount += st.countTokens();
    //System.out.println("Terms Count+:"+termsCount);

    /*
    	String words[]=line.split(" ");
    	PhraseQuery.Builder builder1 = new PhraseQuery.Builder();
    	for(String word:words)
    	{
    		builder1.add("content", word);
    	}
    	PhraseQuery query = builder1.build();
				
    */


    Query query = builder.createPhraseQuery("content", line);
    ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;


    // uncomment these lines to check the correctness of the search results
    //if (hits.length < 1) {
    //    System.out.println("Incorrect search result!");
    //}
    //proximity query is slower to perform and requires more CPU.

    //System.out.println("Number of hits: " + hits.length);
    //System.out.println("Type of query: " + query.getClass().getSimpleName());
   }



   end = System.currentTimeMillis(); in .close();

   ireader.close();
   directory.close();

   // calculate the time it took to search all the terms
   searchTime = (end - start);
  } catch (IOException e) {
   e.printStackTrace();
  }
  latency = (double)(searchTime / termsCount);
  throughput = (double)(((filesLength / 1e9) / (indexTime / (1000.0 * 60.0))) * 60.0);

  System.out.println("No of Threads: 4");
  System.out.println("Data Size: " + (double)(filesLength / 1e9) + " GB");
  System.out.println("Throughput: " + throughput + " GB/hour");
  System.out.println("Number of Documents: " + numDocs);
  System.out.println("IndexTime: " + indexTime + " ms");
  System.out.println("Index Rate: " + (double)((double)(indexTime / 1000) / numDocs) + " sec");
  System.out.println("Search Rate: " + (double)((double)(searchTime / 1000) / numDocs) + " sec");
  System.out.println("IndexSize: " + (double)(indexSize / 1000.0) + " MB");
  System.out.println("SearchTime: " + searchTime + " ms");
  System.out.println("Search Latency: " + latency + " ms");
  System.out.println();
  System.exit(0);

 }
 static double determineGoodBufferSize(double atLeast) {
  double heapHint = Runtime.getRuntime().maxMemory() / (1024 * 1024 * 14);
  //System.out.println("Heap" +heapHint);
  double heapAvailable = Runtime.getRuntime().maxMemory() / (1024 * 1024);
  System.out.println("Heap Space Available (MB)-" + heapAvailable);
  double result = Math.max(atLeast, heapHint);
  return Math.min(result, 700);
 }
}
