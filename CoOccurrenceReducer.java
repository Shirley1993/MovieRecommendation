package collaborativeFiltering;


import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CoOccurrenceReducer extends Reducer<Text, Text, Text, Text> {
	private SortedMap<Integer,String> id_status = new TreeMap<Integer,String>();
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		id_status.clear();
		String line , status;
		int user_id;
		for(Text value:values) {
			line = value.toString();
			String[] tokens = line.split(",");
			user_id = Integer.parseInt(tokens[0]);
			status = tokens[1] + "," + tokens[2] + "," + tokens[3];
			id_status.put(user_id, status);
		}
		int iter1 = 0;
		for (Map.Entry<Integer, String> entry: id_status.entrySet()) {
			int user_id1 = entry.getKey();
			String status1 = entry.getValue();
			int iter2 = 0;
			for (Map.Entry<Integer, String> entry2: id_status.entrySet()) {
				if (iter2++ <= iter1) 
					continue;
				
				int user_id2 = entry2.getKey();
				String status2 = entry2.getValue();
				outputKey.set(Integer.toString(user_id1) + ',' + Integer.toString(user_id2));
				outputValue.set(status1+','+status2);
				context.write(outputKey, outputValue);
			}
			iter1 ++;
		}
		
	}
}
