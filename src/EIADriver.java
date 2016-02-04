import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class EIADriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf(); 
		conf.setBoolean("mapred.compress.map.output", true); 
		conf.setClass("mapred.map.output.compression.codec", SnappyCodec.class, 
				CompressionCodec.class);

		Job job = new Job(conf);
		job.setJarByClass(EIADriver.class);
		job.setJobName("State Gas Data Analysis");

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileOutputFormat.setCompressOutput(job, true); 
		FileOutputFormat.setOutputCompressorClass(job,SnappyCodec.class); 
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		job.setMapperClass(StateMapper.class);
		job.setReducerClass(StateReducer.class);
		job.setCombinerClass(StateCombiner.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}


	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new EIADriver(), args);
		System.exit(exitCode);

	}

}
