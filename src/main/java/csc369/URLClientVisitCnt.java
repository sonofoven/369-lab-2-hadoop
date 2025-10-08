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

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split(" ");
	    IntWritable code = new IntWritable();
	    code.set(Integer.parseInt(sa[8]));
	    context.write(code, one);
        }
    }

    public static class ReducerImpl extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(IntWritable code, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(code, result);
       }
    }

}
