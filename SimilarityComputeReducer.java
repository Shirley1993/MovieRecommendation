package collaborativeFiltering;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SimilarityComputeReducer extends Reducer<Text, Text, Text, DoubleWritable> {

	private DoubleWritable outputValue = new DoubleWritable();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String line;
		double averageX = 0, averageY = 0, rx, ry;
		double dotSum = 0, squareSumX = 0, squareSumY = 0;
		boolean isFirst = true;
		for(Text value:values) {
			line = value.toString();
			String[] tokens = line.split(",");
			if (isFirst) {
				isFirst = false;
				averageX = Double.parseDouble(tokens[2]) / Integer.parseInt(tokens[1]);
				averageY = Double.parseDouble(tokens[5]) / Integer.parseInt(tokens[4]);
			}
			rx = Double.parseDouble(tokens[0]);
			ry = Double.parseDouble(tokens[3]);
			dotSum += (rx - averageX) * (ry - averageY);
			squareSumX += (rx - averageX) * (rx - averageX);
			squareSumY += (ry - averageY) * (ry - averageY);
		}
		outputValue.set(dotSum / (Math.sqrt(squareSumX) * Math.sqrt(squareSumY)));
		context.write(key, outputValue);
	}
}
