package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		FileSystem fd = FileSystem.get(conf);

		Job job = new Job(conf, "Job One");
		Boolean jobChain = false;
		Job job2 = new Job(conf, "Job Two");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		fd.delete(new Path(otherArgs[1]), true);

	if (otherArgs.length < 3) {
		System.out.println("Expected parameters: <job class> <input dir> <output dir>");
		System.exit(-1);

	} else if ("URLReqCnt".equalsIgnoreCase(otherArgs[0])) { // Part 1 //
		jobChain = true;

		job.setMapperClass(URLReqCnt.MapperImpl.class);
		job.setReducerClass(URLReqCnt.ReducerImpl.class);
		job.setOutputKeyClass(URLReqCnt.OUTPUT_KEY_CLASS_TEMP);
		job.setOutputValueClass(URLReqCnt.OUTPUT_VALUE_CLASS_TEMP);

		job2.setMapperClass(URLReqCnt.MapperImpl2.class);
		job2.setReducerClass(URLReqCnt.ReducerImpl2.class);
		job2.setOutputKeyClass(URLReqCnt.OUTPUT_KEY_CLASS_FINAL);
		job2.setOutputValueClass(URLReqCnt.OUTPUT_VALUE_CLASS_FINAL);

	} else if ("CodeReqCnt".equalsIgnoreCase(otherArgs[0])) { // Part 2 //
		job.setReducerClass(CodeReqCnt.ReducerImpl.class);
		job.setMapperClass(CodeReqCnt.MapperImpl.class);
		job.setOutputKeyClass(CodeReqCnt.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CodeReqCnt.OUTPUT_VALUE_CLASS);

	} else if ("HostnameBytes".equalsIgnoreCase(otherArgs[0])) { // Part 3
		job.setReducerClass(HostnameBytes.ReducerImpl.class);
		job.setMapperClass(HostnameBytes.MapperImpl.class);
		job.setOutputKeyClass(HostnameBytes.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(HostnameBytes.OUTPUT_VALUE_CLASS);

	} else if ("URLClientVisitCnt".equalsIgnoreCase(otherArgs[0])) { // Part 4
		job.setReducerClass(URLClientVisitCnt.ReducerImpl.class);
		job.setMapperClass(URLClientVisitCnt.MapperImpl.class);
		job.setOutputKeyClass(URLClientVisitCnt.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(URLClientVisitCnt.OUTPUT_VALUE_CLASS);

	} else if ("DateReqs".equalsIgnoreCase(otherArgs[0])) { // Part 5
		job.setReducerClass(DateReqs.ReducerImpl.class);
		job.setMapperClass(DateReqs.MapperImpl.class);
		job.setOutputKeyClass(DateReqs.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(DateReqs.OUTPUT_VALUE_CLASS);

	} else if ("BytesInDay".equalsIgnoreCase(otherArgs[0])) { // Part 6
		job.setReducerClass(BytesInDay.ReducerImpl.class);
		job.setMapperClass(BytesInDay.MapperImpl.class);
		job.setOutputKeyClass(BytesInDay.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(BytesInDay.OUTPUT_VALUE_CLASS);

	} else {
		System.out.println("Unrecognized job: " + otherArgs[0]);
		System.exit(-1);
	}

	Boolean exitStatus = true;

	if (jobChain){
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path("temp"));
		job.waitForCompletion(true);

		FileInputFormat.addInputPath(job2, new Path("temp"));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

		exitStatus = job2.waitForCompletion(true);
		fd.delete(new Path("temp"), true);

	} else {
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		exitStatus = job.waitForCompletion(true);
	}

	int status = exitStatus ? 0 : 1;
	System.exit(status);
	}
}
