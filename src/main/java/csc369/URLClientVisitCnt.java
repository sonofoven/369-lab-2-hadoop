package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class URLClientVisitCnt {

    public static final Class OUTPUT_KEY_CLASS_TEMP = Text.class;
    public static final Class OUTPUT_VALUE_CLASS_TEMP = IntWritable.class;

    public static final Class OUTPUT_KEY_CLASS_FINAL = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS_FINAL = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	public static final Text matchText = new Text("/favicon.ico");

	private final IntWritable one = new IntWritable(1);
	private static Text hostname = new Text();
	private static Text url = new Text();

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split(" ");
	    url.set(sa[6]);
		hostname.set(sa[0]);
		if (url.equals(matchText)){
			context.write(hostname, one);
		}
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, IntWritable, Text> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text hostname, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(result, hostname);
       }
    }


    public static class MapperImpl2 extends Mapper<LongWritable, Text, IntWritable, Text> {
	private final IntWritable count = new IntWritable();
	private static Text hostname = new Text();

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\\s+");
	    hostname.set(sa[1]);
	    count.set(Integer.parseInt(sa[0]));
	    context.write(count, hostname);
        }
    }

    public static class ReducerImpl2 extends Reducer<IntWritable, Text, IntWritable, Text> {
	private final Text hostname = new Text();
    
        @Override
	protected void reduce(IntWritable count, Iterable<Text> hostnames,
			      Context context) throws IOException, InterruptedException {

			for (Text h : hostnames){
				hostname.set(h);
            	context.write(count, hostname);
			}
       }
    }
}



