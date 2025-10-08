package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class URLReqCnt {

    public static final Class OUTPUT_KEY_CLASS_TEMP = Text.class;
    public static final Class OUTPUT_VALUE_CLASS_TEMP = IntWritable.class;

    public static final Class OUTPUT_KEY_CLASS_FINAL = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS_FINAL = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split(" ");
	    Text url = new Text();
	    url.set(sa[6]);
	    context.write(url, one);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, IntWritable, Text> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text url, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(result, url);
       }
    }


    public static class MapperImpl2 extends Mapper<LongWritable, Text, IntWritable, Text> {
	private final IntWritable count = new IntWritable();

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\\s+");
	    Text url = new Text();
	    url.set(sa[1]);
	    count.set(Integer.parseInt(sa[0]));
	    context.write(count, url);
        }
    }

    public static class ReducerImpl2 extends Reducer<IntWritable, Text, IntWritable, Text> {
    
        @Override
	protected void reduce(IntWritable count, Iterable<Text> urls,
			      Context context) throws IOException, InterruptedException {

			Text url = new Text();
			for (Text h : urls){
				url.set(h);
            	context.write(count, url);
			}
       }
    }
}



