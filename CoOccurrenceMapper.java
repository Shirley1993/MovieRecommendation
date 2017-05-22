package collaborativeFiltering;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CoOccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text movieID = new Text();
	private Text status = new Text();
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split("\t");
		if (tokens.length != 5)
			return;
		movieID.set(tokens[1]);
		status.set(tokens[0] + ',' + tokens[2] + ',' + tokens[3] + ',' + tokens[4]);
		context.write(movieID, status);
	}
}
