/*
* 
* Author   :  Suresh Appana
* File Name:  TermFrequency.java
* Purpose: To find the logarithmically scaled term frequency
*
*/
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
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


public class TermFrequency extends Configured implements Tool {

   //Creating instance to the looger class
   private static final Logger LOG = Logger .getLogger( TermFrequency.class);

   //Main method. The first method to be executed by the program
   public static void main( String[] args) throws  Exception {
      //Calling the run() which schedules MapReduce job.
      int res  = ToolRunner .run( new TermFrequency(), args);
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
      //Creating instance to the Job class
      Job job  = Job .getInstance(getConf(), " termfrequency ");
	  //Setting the jar of that class
      job.setJarByClass( this .getClass());

	  //Input locations for the job. i.e., the path of input files
      FileInputFormat.addInputPaths(job,  args[0]);
	  //Output location for the job. i.e., the path where output file will be generated. The path should not be existed prior to execution if this step.
	  //If the path already existed then it will give error.
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
	  //Setting the mapper class
      job.setMapperClass( Map .class);
	  //Setting the reducer class
      job.setReducerClass( Reduce .class);
	  //Sets the output key for the job
      job.setOutputKeyClass( Text .class);
	  //Sets the output value for the job
      job.setOutputValueClass( IntWritable .class);

	  //Waits till the job completed
      return job.waitForCompletion( true)  ? 0 : 1;
   }

   /*
	* 
	* Class Name: Map
	* Purpose: To perform the Map task
	*
	*/
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

	  //Delimeter for identifying the words
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

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
	* Class Name: Reduce
	* Purpose: To perform the Reduce task
	*
	*/
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
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
		term_freq = 0;
		 //writing the output
         context.write(word,  new DoubleWritable(term_freq));
      }
   }
}

