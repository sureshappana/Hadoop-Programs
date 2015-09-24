/*
* 
* Author   :  Suresh Appana
* File Name:  TFIDF.java
* Purpose: To find the frequency of words
*
*/

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;
import java.lang.NumberFormatException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class TFIDF extends Configured implements Tool {

   //Creating instance to the looger class
   private static final Logger LOG = Logger .getLogger( TFIDF.class);
   private static final int TOTAL_FILE_COUNT = 0;
   private static final String temporary_path = "temp_path";

   //Main method. The first method to be executed by the program
   public static void main( String[] args) throws  Exception {
      //Calling the run() which schedules MapReduce job.
      int res  = ToolRunner .run( new TFIDF(), args);
      System .exit(res);
   }

   /*
	* 
	* Function Name: run
	* Input value:  String[]
	* Return value: int
	* Purpose: To schedule the MapReduce job
	*
	*/
   public int run( String[] args) throws  Exception {
      //Creating instance to the Job class to perform job1
      Job job1  = Job .getInstance(getConf(), " wf ");
	  //Setting the jar of that class
      job1.setJarByClass( this .getClass());

	  //Input locations for the job. i.e., the path of input files
      FileInputFormat.addInputPaths(job1,  args[0]);
      //Output location for the job. temporary path is created wich acts as an intermediate
	  //location to communicate between two mapreduce jobs
      FileOutputFormat.setOutputPath(job1,  new Path(temporary_path));
	  
	  //Receiving the total file count from user as an argument.
      TOTAL_FILE_COUNT = Integer.parseInt(arg[2]);
	  
	  //Setting the mapper class for job1
      job1.setMapperClass( Map1 .class);
      //Setting the reducer class for job1
      job1.setReducerClass( Reduce1 .class);
      //Setting the output key class
      job1.setOutputKeyClass( Text .class);
	  //setting the output value class
      job1.setOutputValueClass( IntWritable.class);

      //waits till job1 completed
      job1.waitForCompletion( true);

      //Instantiating job2
      Job job2  = Job .getInstance(getConf(), " tfidf ");
	  //Setting the jar of that class
      job2.setJarByClass( this .getClass());
      //Input locations for the job. i.e., the path of input files
      FileInputFormat.addInputPaths(job2,  temporary_path);
      
	  //Output location for the job. i.e., the path where output file will be generated. The path should not be existed prior to execution if this step.
	  //If the path already existed then it will give error.
      FileOutputFormat.setOutputPath(job2,  new Path(args[1]));
      
	  //Setting the mapper class for job2
      job2.setMapperClass( Map2 .class);
      //Setting the reducer class for job2
      job2.setReducerClass( Reduce2 .class);

      //Setting the output key class for job2
      job2.setOutputKeyClass( Text .class);
      //Setting the output key value for job2
      job2.setOutputValueClass( Text .class);

      // wait till job2 completed
      return job2.waitForCompletion( true)  ? 0 : 1;
   }
    /*
	* 
	* Class Name: Map1
	* Purpose: To perform the Map task of job1
	*
	*/
   public static class Map1 extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

	  //Delimeter for identifying the words
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

		 //Assigning the line variable with the line text.
         String line  = lineText.toString();
         Text currentWord  = new Text();
		 //Getting the file name
         String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		 //Iterating over the words in the line

         for ( String word  : WORD_BOUNDARY .split(line)) {
		    //if the word is empty the continue with next word.
            if (word.isEmpty()) {
               continue;
            }
			//setting the currentword to write 
            currentWord  = new Text(word+"#####"+fileName);
			//writing the current word to the ouput key:currentword and value:1
            context.write(currentWord,one);
         }
      }
   }

   /*
	* 
	* Class Name: Reduce1
	* Purpose: To perform the Reduce task
	*
	*/
   public static class Reduce1 extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override 
	  	   /*
		* 
		* Function Name: reduce
		* Input value:  Text,  Iterable<IntWritable>,  Context
		* Return value: void
		* Purpose: actual function which performs reduce task
		*
		*/
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
		 //Counting the list of Iterable who are having the same key.
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
	 double term_freq;
	  //WF(t,d) = 1 + log10(TF(t,d)) if TF(t,d) > 0, and 0 otherwise
	 if(sum>0)
		term_freq = 1+Math.log10(sum);
         else
		term_freq = 0.0;
		//writing the output
         context.write(word,  new DoubleWritable(term_freq));
      }
   }

   /*
	* 
	* Class Name: Map2
	* Purpose: To perform the Map task of job2
	*
	*/
public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private Text word  = new Text();
	   /*
		* 
		* Function Name: map
		* Input value:  LongWritable,  Text,  Context
		* Return value: void
		* Purpose: actual function which performs map task
		*
		*/


      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
		
		//Assigning the line variable with the line text.
         String line  = lineText.toString();
        
		 Text currentWord  = new Text();
         Text fileName  = new Text();
         double frequency = 0.0;
	    
		//Splitting the line
		 String[] tokens = line.split("\t|#####");
		 try{
		    //converting the string to double value
		     frequency = Double.parseDouble(tokens[2]);
		    } catch(NumberFormatException ex){ 
			ex.printStackTrace();
	              }
			//writing the output of map function
         context.write(new Text(tokens[0]),new Text(tokens[1]+"="+frequency));
      } 
   }

   /*
	* 
	* Class Name: Reduce2
	* Purpose: To perform the Reduce task2
	*
	*/
   public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
       /*
		* 
		* Function Name: reduce
		* Input value:  Text,  Iterable<Text>,  Context
		* Return value: void
		* Purpose: actual function which performs reduce task
		*
		*/
      @Override 
      public void reduce( Text word,  Iterable<Text > texts,  Context context)
         throws IOException,  InterruptedException {

      // Caching the values
	 List<String> values = new ArrayList<String>();
         for (Text text: texts) {
		values.add(text.toString());
         }


         //Finding the idf
         double idf =  Math.log10(((double)TOTAL_FILE_COUNT)/values.size()); 
         for ( int i = 0; i < values.size(); i++) {
		 //Splitting the line to extract values
		String[] tokens = values.get(i).split("=");
                double tf =0.0;
              try{
			  //converting to double
		     tf = Double.parseDouble(tokens[1]);

		    } catch(NumberFormatException ex){ 
			ex.printStackTrace();
	              }
        //writing the output
                context.write(new Text(word+"#####"+tokens[0]), new DoubleWritable(tf*idf));
         } 
      }
   }
}

