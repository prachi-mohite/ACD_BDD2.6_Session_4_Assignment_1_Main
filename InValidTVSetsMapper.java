import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InValidTVSetsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	//overriding the map method from mapper interface
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException 
	{
		String rowDetails = value.toString();
		String[] parts = rowDetails.split("|");
		if (parts[0].equals("NA")) 
		{
			context.write(new Text(parts[0]), new Text("-This is invalid entry"));
		}
	}

}
