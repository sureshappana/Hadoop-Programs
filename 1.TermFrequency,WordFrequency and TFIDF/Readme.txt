Author: Suresh Appana

 There are totally three programs with the names DocWordCount.java, TermFrequency.java and TFIDF.java
 
 Copy all the input files to the user folder. Open terminal at that location
 Copy the input files to hadoop system.
 
 hadoop fs -put file* /user/suresh/wordcount/input
 
 DocWordCount.java:
 ------------------
 This file calcualtes the term frequency. Term frequency is the number of times a particular word t occurs in a document d.
	TF(t, d) = No. of times t appears in document d
	
	Execution steps:
	---------------
		1. Run the following command to delete if the output folder already existed
			
			hadoop fs -rm -r /user/suresh/wordcount/output
			
		2. Compile the DocWordCount class. 
			
			mkdir -p build
			javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint 
		
		3. Create a JAR file for the DocWordCount application.
		
			jar -cvf DocWordCount.jar -C build/ . 
		
		4.Run the DocWordCount application from the JAR file, passing the paths to the input and output directories in HDFS.
			
			hadoop jar DocWordCount.jar org.myorg.DocWordCount /user/suresh/wordcount/input /user/suresh/wordcount/output 
		   
		5. To print the output
		
			hadoop fs -cat /user/suresh/wordcount/output/*
			
			
			
TermFrequency.java:
------------------
 Inverse Document Frequency:
	The inverse document frequency (IDF) is a measure of how common or rare a term is across all documents in the collection. It is the logarithmically scaled fraction of the
	documents that contain the word, and is obtained by taking the logarithm of the ratio of the total number of documents to the number of documents containing the term.
	IDF(t) = log10 (Total number of documents / Num. of documents containing term t).	

	Execution steps:
	---------------
		1. Run the following command to delete if the output folder already existed
			
			hadoop fs -rm -r /user/suresh/wordcount/output
			
		2. Compile the TermFrequency class. 
			
			mkdir -p build
			javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java -d build -Xlint 
		
		3. Create a JAR file for the TermFrequency application.
		
			jar -cvf TermFrequency.jar -C build/ . 
		
		4.Run the TermFrequency application from the JAR file, passing the paths to the input and output directories in HDFS.
			
			hadoop jar TermFrequency.jar org.myorg.TermFrequency /user/suresh/wordcount/input /user/suresh/wordcount/output 
		   
		5. To print the output
		
			hadoop fs -cat /user/suresh/wordcount/output/*
			
			
TFIDF.java:
-----------
TF-IDF:
	Term frequency–inverse document frequency (TF-IDF) is a numerical statistic that is intended to reflect how important a word is to a document in a collection or corpus of
	documents. It is often used as a weighting factor in information retrieval and text mining.
			
			TF-IDF(t, d) = WF(t,d) * IDF(t)	

	This program is going to raise two MapReduce jobs. First MapReduce job is same as TermFrequency.java then
	
	the output of Map2 is of the format:
		<"yellow ", "file2.txt=1.0”> <"Hadoop", "file2.txt=1.0”> <”is”, “file2.txt=1.0”>
		<”elephant”,“file2.txt=1.0”> <"yellow", "file1.txt=1.0”> <"Hadoop", "file1.txt=1.3010299956639813">
		<”is”, “file1.txt=1.0”> <”an”, “file2.txt=1.0”> 
	
	Then the output of Reduce2 is of the format:
	
		<"Hadoop#####file1.txt, 0.0"> <"is#####file1.txt, 0.0"> <"yellow#####file1.txt, 0.0">
		<"yellow#####file2.txt, 0.0"> <"Hadoop#####file2.txt, 0.0"> <"is#####file2.txt, 0.0">
		<"an#####file2.txt, 0.3010299956639812"> <"elephant#####file2.txt, 0.3010299956639812">
	
	
	
	Execution steps:
	---------------
		1. Run the following command to delete if the output folder already existed
			
			hadoop fs -rm -r /user/suresh/wordcount/output
			
		2. Compile the TFIDF class. 
			
			mkdir -p build
			javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TFIDF.java -d build -Xlint 
		
		3. Create a JAR file for the TFIDF application.
		
			jar -cvf TFIDF.jar -C build/ . 
		
		4.Run the TFIDF application from the JAR file, passing the paths to the input and output directories in HDFS.
			
			hadoop jar TFIDF.jar org.myorg.TFIDF /user/suresh/wordcount/input /user/suresh/wordcount/output 4
			
			where 4 indicates the number of files. If the input number of files are different then change this number
		   
		5. To print the output
		
			hadoop fs -cat /user/suresh/wordcount/output/*