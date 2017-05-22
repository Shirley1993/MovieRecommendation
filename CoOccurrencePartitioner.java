package collaborativeFiltering;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class CoOccurrencePartitioner<Text,IntWritable> extends Partitioner<Text, IntWritable> implements  Configurable {

	private Configuration configuration;
	
	@Override
	public void setConf(Configuration configuration) {
		this.configuration = configuration;
	}
	
	@Override
	public Configuration getConf() {
		return configuration;
	}
	
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		int movie_id = Integer.parseInt(key.toString());
//		if (movie_id < 5000)
//			return 0;
//		else if (movie_id < 10000)
//			return 1 % numReduceTasks;
//		else if (movie_id < 15000)
//			return 2 % numReduceTasks;
//		else
//			return 3 % numReduceTasks;
		if (movie_id < 2000)
			return 0;
		else if (movie_id < 4000)
			return 1 % numReduceTasks;
		else if (movie_id < 6000)
			return 2 % numReduceTasks;
		else if (movie_id < 8000)
			return 3 % numReduceTasks;
		else if (movie_id < 10000)
			return 4 % numReduceTasks;
		else if (movie_id < 12000)
			return 5 % numReduceTasks;
		else if (movie_id < 14000)
			return 6 % numReduceTasks;
		else if (movie_id < 16000)
			return 7 % numReduceTasks;
		else 
			return 8 % numReduceTasks;
	}
}
