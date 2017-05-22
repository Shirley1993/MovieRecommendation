package collaborativeFiltering;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public class CFDriver extends Configured implements Tool{
	
	private static final String outputPath = "/home/training/training_materials/analyst/exercises/Collaborative_Filtering/JOB1_output";
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration() , new CFDriver() , args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {

    
	    if (args.length != 2) {
	      System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n",getClass().getSimpleName());
	      return -1;
	    }
	
	    /*
	     * Instantiate a Job object for your job's configuration. 
	     */
	    Configuration conf = getConf();
	    FileSystem fs = FileSystem.get(conf);
	    
	    Job job = new Job(conf);
	    
	    job.setJarByClass(CFDriver.class);
	    
	    job.setJobName("JOB1 Co-occurrence");
	
	    //job.setNumReduceTasks(9);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(job,new Path(args[0]));
	    FileOutputFormat.setOutputPath(job,new Path(outputPath));
	    
	    FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
	    
	    job.setMapperClass(CoOccurrenceMapper.class);
	    job.setReducerClass(CoOccurrenceReducer.class);
	    //job.setPartitionerClass(CoOccurrencePartitioner.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	
	    //Second job
	    
	    job.waitForCompletion(true);
	    
	    Job job2 = new Job(conf);
	    
	    job2.setJarByClass(CFDriver.class);
	    
	    job2.setJobName("JOB2 Similarity");
	    
	    job2.setInputFormatClass(SequenceFileInputFormat.class);
	    
	    FileInputFormat.setInputPaths(job2,new Path(outputPath));
	    FileOutputFormat.setOutputPath(job2,new Path(args[1]));
	    job2.setMapperClass(SimilarityComputeMapper.class);
	    job2.setReducerClass(SimilarityComputeReducer.class);
	    job2.setNumReduceTasks(1);
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(DoubleWritable.class);
	    
	    return job2.waitForCompletion(true) ? 0 : 1;
	}
}


