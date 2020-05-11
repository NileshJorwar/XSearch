import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.store.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.MergeScheduler;
import java.io.*;
import java.util.*;


import java.nio.file.Paths;

class MyThread extends Thread{
	ArrayList<Document> docs;
	//ArrayList<InputStreamReader> f_o;
	//ArrayList<FileInputStream> f_i;
	ArrayList<String> inputFiles;
	IndexWriter iwriter;
	int num_threads;
	int tid;
	MyThread(ArrayList<Document> docs, IndexWriter iwriter, int num_threads, int tid, ArrayList<String> inputFiles){
		this.docs = docs;
		this.iwriter = iwriter;
		this.num_threads = num_threads;
		this.tid = tid;
		this.inputFiles=inputFiles;
		//this.f_o = f_o;
		//this.f_i = f_i;
	}
	public void run(){
		//System.out.println("Thread "+tid);
		try{
			//for (Document doc:docs){
			for (int i =tid; i < 1000; i += num_threads) {
				File file = new File(inputFiles.get(i));
				//filesLength+=file.length();
				FileInputStream fis = new FileInputStream(file);
                InputStreamReader isr = new InputStreamReader(fis);
				BufferedReader reader =new BufferedReader(new FileReader(inputFiles.get(i)));
				String line = reader.readLine();
				while(line != null){
					String[] data = line.split(" ");
					Document document = new Document();
					Field contentField1 = new Field("inode", data[0], TextField.TYPE_STORED);
					Field contentField2 = new Field("gen_num1", data[1], TextField.TYPE_STORED);
					Field contentField3 = new Field("snap_id", data[2], TextField.TYPE_STORED);
					Field contentField4 = new Field("fileset_name", data[4], TextField.TYPE_STORED);
					Field contentField5 = new Field("gen_num2", data[5], TextField.TYPE_STORED);
					Field contentField6 = new Field("user_id", data[8], TextField.TYPE_STORED);
					Field contentField7 = new Field("group_id", data[9], TextField.TYPE_STORED);
					Field contentField8 = new Field("f_name", data[15], TextField.TYPE_STORED);
					Field contentField9 = new Field("content", isr, TextField.TYPE_NOT_STORED);
					int x=1;
					Field filenameField = new Field("filename", file.getName(), StoredField.TYPE);
					Field filepathField = new Field("filepath", file.getCanonicalPath(), StoredField.TYPE);
					line = reader.readLine();if(x==1){break;}
					document.add(contentField1);
					document.add(contentField2);
					document.add(contentField3);
					document.add(contentField4);
					document.add(contentField5);
					document.add(contentField6);
					document.add(contentField7);
					document.add(contentField8);
					document.add(contentField9);
					document.add(filenameField);
					document.add(filepathField);
					iwriter.addDocument(document);}
				
				iwriter.addDocument(docs.get(i));
				isr.close();
				fis.close();
			}
		}
		catch(Exception e){
			System.out.println("Thread: "+e);
		}
	}
}

class XSearchData {
    public static void main(String[] args) {
        Analyzer analyzer;
        FSDirectory directory;
        IndexWriterConfig config;
        IndexWriter iwriter;
        DirectoryReader ireader;
        IndexSearcher isearcher;
        QueryBuilder builder;
        RandomAccessFile in;
        ArrayList<String> inputFiles;
        String termsFile;
        ArrayList<Document> documents;
		ConcurrentMergeScheduler scheduler;
		ArrayList<InputStreamReader> f_o =new ArrayList();
		ArrayList<FileInputStream> f_i =new ArrayList();
        String line;
        long start, end;
        long indexTime, indexSize, searchTime;
		double filesLength=0L;
		int thr_cnt = Integer.parseInt(args[2]);
		System.out.println("Iteration of Threads: "+ thr_cnt);
        indexTime = 0;
        indexSize = 0;
        searchTime = 0;
        try {
            // read the file paths from the input file
            inputFiles = new ArrayList<String>();
            in = new RandomAccessFile(args[0], "r");
			int rfc=0;
           
			while ((line = in.readLine()) != null) {
				if(rfc>1001) {break;}
               
				inputFiles.add(line);
				rfc++;
            }
            in.close();
            
			/***************************************************/
			
			/**************************************************/
			
            // create a list of document that are going to be indexed
            documents = new ArrayList<Document>();
			int f_cnt=0;
            for (String inputFile : inputFiles) {
				if (f_cnt>1000){break;}
                File file = new File(inputFile);
				FileInputStream fis = new FileInputStream(file);
                InputStreamReader isr = new InputStreamReader(fis);
				filesLength+=file.length();
				//BufferedReader reader = 
                Document document = new Document();

                Field contentField = new Field("content", isr,
                        TextField.TYPE_NOT_STORED);
                Field filenameField = new Field("filename", file.getName(), StoredField.TYPE);
                Field filepathField = new Field("filepath", file.getCanonicalPath(), StoredField.TYPE);

                document.add(contentField);
                document.add(filenameField);
                document.add(filepathField);

                documents.add(document);
				f_cnt+=1;
				//isr.close();
				//fis.close();
            }
	    
			filesLength = filesLength/1e9;
			System.out.println("Data set Size: "+filesLength);

            // use the standard text analyzer
            analyzer = new StandardAnalyzer();

            // store the index in main memory (RAM)
           	File inddir=new File ("../Data/index/ind");
			directory = FSDirectory.open(Paths.get("../Data/index/ind"));

            // create and index writer
            config = new IndexWriterConfig(analyzer);
            //iwriter = new IndexWriter(directory, config);
			
			//Multi-Threaded approach
			//	int thr_cnt = Integer.parseInt(args[2]);			

			scheduler = new ConcurrentMergeScheduler();
			scheduler.setMaxMergesAndThreads(thr_cnt,thr_cnt);
			config.setMergeScheduler(scheduler);
			
			iwriter = new IndexWriter(directory, config);
			//MyThread mythread[] = new MyThread[4];
			Vector<MyThread> mythread = new Vector<MyThread>();
			//ArrayList<MyThread> mythread = new ArrayList<MyThread>();
			
			start = System.currentTimeMillis();
			try{
				for (int tid = 0; tid < thr_cnt; tid++) {
						MyThread th = new MyThread(documents, iwriter, thr_cnt, tid, inputFiles);
						mythread.add(th);
						th.start();
				}
				for (MyThread th: mythread) {
					th.join();
				}
			}
			catch(Exception e){
				System.out.println(e);
			}
			
            iwriter.commit();
            iwriter.close();
			
      	    end = System.currentTimeMillis();
            
            // calculate the time taken to index the files
            indexTime = (end - start);

            // create an index reader
            ireader = DirectoryReader.open(directory);
            isearcher = new IndexSearcher(ireader);
            builder = new QueryBuilder(analyzer);

            // read the terms from the second input file and search the index
            in = new RandomAccessFile(args[1], "r");
            start = System.currentTimeMillis();
            int incor_cnt=0;
			int cor_cnt=0;
				
			while ((line = in.readLine()) != null) {
                Query query = builder.createBooleanQuery("content", line);
                ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
                // uncomment these lines to check the correctness of the search results
                if (hits.length < 1) {
					incor_cnt+=1;
                    //System.out.println("Incorrect search result!");
                }
				else{cor_cnt+=1;}
				
					//System.out.println("Correct search result!");
            }
			System.out.println("Correct:Incorrect = "+cor_cnt+":"+incor_cnt);
            end = System.currentTimeMillis();
            in.close();

            ireader.close();
            directory.close();

            // calculate the time it took to search all the terms
            searchTime = (end - start);
        } catch (IOException e) {
            e.printStackTrace();
        }

	double throughput = ((filesLength)/(indexTime/60000.0))*60.0;

        System.out.println("IndexTime: " + indexTime + " ms");
        //System.out.println("IndexSize: " + indexSize + " kB");
        System.out.println("SearchTime: " + searchTime + " ms");
	System.out.println("Throughput: " + throughput + " GB/hour");
	//System.out.println("Threads: " + thr_cnt );
        System.exit(0);  
    }
}
