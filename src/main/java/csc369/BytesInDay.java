package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class BytesInDay {

    public static final Class OUTPUT_KEY_CLASS_TEMP = Text.class;
    public static final Class OUTPUT_VALUE_CLASS_TEMP = IntWritable.class;

    public static final Class OUTPUT_KEY_CLASS_FINAL = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS_FINAL = Text.class;

	public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable bytes = new IntWritable();
	private final Text dateText = new Text();

		@Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {

		String line = value.toString();

		int start = line.indexOf('[') + 1;
		int end = line.indexOf(':', start);
		String date = line.substring(start, end);
		
		String[] dateParts = date.split("/");

		String day = dateParts[0];
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

		int dayNum = Integer.parseInt(day);

		String dateConcat = String.format("%s-%02d-%02d", year, monthNum, dayNum);
		dateText.set(dateConcat);

	    String[] sa = value.toString().split(" ");
	    bytes.set(Integer.parseInt(sa[9]));

		context.write(dateText, bytes);
		}
	}

	public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
	
		@Override
	protected void reduce(Text dateText, Iterable<IntWritable> bytes,
				  Context context) throws IOException, InterruptedException {
			int totBytes = 0;
			Iterator<IntWritable> itr = bytes.iterator();
		
			while (itr.hasNext()){
				totBytes += itr.next().get();
			}
			result.set(totBytes);
			context.write(dateText, result);
	   }
	}


    public static class MapperImpl2 extends Mapper<LongWritable, Text, IntWritable, Text> {
	private final IntWritable count = new IntWritable();

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\\s+");
	    Text date = new Text();
	    date.set(sa[0]);
	    count.set(Integer.parseInt(sa[1]));
	    context.write(count, date);
        }
    }

    public static class ReducerImpl2 extends Reducer<IntWritable, Text, IntWritable, Text> {
    
        @Override
	protected void reduce(IntWritable count, Iterable<Text> dates,
			      Context context) throws IOException, InterruptedException {

			Text date = new Text();
			for (Text h : dates){
				date.set(h);
            	context.write(count, date);
			}
       }
    }
}
