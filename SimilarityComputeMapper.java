package collaborativeFiltering;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SimilarityComputeMapper extends Mapper<Text, Text, Text, Text> {
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		//String line = value.toString();
		//String[] tokens = line.split("\t");
		//context.write(new Text(tokens[0]), new Text(tokens[1]));
		context.write(new Text(key), new Text(value));
	}
}
