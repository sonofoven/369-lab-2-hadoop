package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class HostnameBytes {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	public static final Text matchText = new Text("cpe-203-51-137-224.vic.bigpond.net.au");
	public final IntWritable bytes = new IntWritable();
	public final Text hostname = new Text();

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split(" ");

	    hostname.set(sa[0]);
	    bytes.set(Integer.parseInt(sa[9]));
		if (hostname.equals(matchText)){
			System.out.println(hostname + ": " + bytes);
	    	context.write(hostname, bytes);
		}
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
