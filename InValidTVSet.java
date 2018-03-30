import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class InValidTVSet  {
	public static void main (String[] args) throws Exception{
		
		//check for input given to driver program
		if (args.length < 2)
		{
			System.err.println("Error: Input not provided properly");
			System.exit(-1);
		}
		
		//set configuration
		Configuration conf = new Configuration();
		//create a new job with the above configuration
		Job job  = new Job(conf);
		
		//check for input argument 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//set the mapper and reducer class : as we have to just fecth invalid records only map job is required
		//set reducer task to ZERO
		job.setNumReduceTasks(0);
		
		//setting the mapper class
		job.setMapperClass(InValidTVSetsMapper.class);
		
		//setting the input format class
		job.setInputFormatClass(TextInputFormat.class);
		//setting output format class
		job.setOutputFormatClass(TextOutputFormat.class);
		
		  //set up the output key and value classes 	
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //execute the job
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
	

}
