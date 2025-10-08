package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DateReqs {

	public static final Class OUTPUT_KEY_CLASS = Text.class;
	public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

	public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);
	private final Text dateText = new Text();

		@Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {

		String line = value.toString();

		int start = line.indexOf('[') + 1;
		int end = line.indexOf(':', start);
		String date = line.substring(start, end);
		
		String[] dateParts = date.split("/");

		String month = dateParts[1];
		String year = dateParts[2];

		String[] months = {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};

		int monthNum = 0;

		for (int i = 0; i < months.length; i++) {
			if (months[i].equalsIgnoreCase(month)) {
				monthNum = i + 1;
				break;
			}
		}

		String dateConcat = String.format("%s-%02d", year, monthNum);
		dateText.set(dateConcat);

		context.write(dateText, one);
		}
	}

	public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
	
		@Override
	protected void reduce(Text hostname, Iterable<IntWritable> bytes,
				  Context context) throws IOException, InterruptedException {
			int totBytes = 0;
			Iterator<IntWritable> itr = bytes.iterator();
		
			while (itr.hasNext()){
				totBytes += itr.next().get();
			}
			result.set(totBytes);
			context.write(hostname, result);
	   }
	}

}
