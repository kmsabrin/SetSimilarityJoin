/************************************************************/
/** Parallel Set Similarity Join in MapReduce ( -BASIC- ) ***/
/** Author: Kaeser Md. Sabrin *******************************/
/** Date: 2 February, 2013 **********************************/
/** Hapoop Version 0.20.2 ***********************************/
/************************************************************/

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class KJoin extends Configured implements Tool {
	/*********************************************************************************/
	/******** Mapper and Reducer classes for Phase 1 *********************************/
	/******** Input: Text files in <tid text> format *********************************/
	/******** Output: Text files in <token tid> format *******************************/
	/*********************************************************************************/

	public static class MapClass1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static Set<String> stopWords = new HashSet<String>(20);
		private static Text token = new Text();
		private static IntWritable tid = new IntWritable();
		private int poison = 0;

		public void setup(Context context) {
			stopWords.add("from");
			stopWords.add("by");
			stopWords.add("using");
			stopWords.add("an");
			stopWords.add("with");
			stopWords.add("to");
			stopWords.add("based");
			stopWords.add("on");
			stopWords.add("in");
			stopWords.add("the");
			stopWords.add("end");
			stopWords.add("for");
			stopWords.add("of");
			// ??
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());

			// tid.set( Integer.parseInt( itr.nextToken() ) );
			tid.set(123);

			while (itr.hasMoreTokens()) {
				String s = itr.nextToken();
				token.set(s);
				context.write(token, tid);

				// if ( s.length() > 1 && stopWords.contains(s) == false )
				// {
				// token.set( s );
				// context.write( token, tid );
				// }
			}

			// if ( ++poison % 50 == 0 )
			// {
			// token.set( "of" );
			// context.write( token, tid );
			// poison = 0;
			// }
		}
	}

	public static class Reduce1 extends
			Reducer<Text, IntWritable, IntWritable, IntWritable> {
		private static IntWritable k1 = new IntWritable();
		private static IntWritable k2 = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// ArrayList<Integer> aList = new ArrayList<Integer>();

			for (IntWritable val : values) {
				int v = val.get();

				// k1.set( v );
				// // WHAT TO DO??
				// for ( int i = 0; i < aList.size(); ++i )
				// {
				// int t = aList.get(i);
				// k2.set( t );
				// if ( v < t ) context.write( k1, k2 );
				// else context.write( k2, k1 );
				// }

				// aList.add(v);
			}
		}
	}

	/*********************************************************************************/
	/******** Mapper and Reducer classes for Phase 2 *********************************/
	/******** Input: Text files in <tid1 tid2> format ********************************/
	/******** Output: Text files in <tid1,tid2 OVERLAP_COUNT> format **************/
	/*********************************************************************************/

	public static class MapClass2 extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String k = itr.nextToken() + "," + itr.nextToken();
			context.write(new Text(k), one);
		}
	}

	/*-------------------------------------------------------------------------------*/

	public static class Reduce2 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int s = 0;
			for (IntWritable val : values) {
				s += val.get();
			}

			context.write(key, new IntWritable(s));
		}
	}

	/**********************************************************************************/
	// /////////////////////////////////
	// usage in out1 out2 /////////////
	// /////////////////////////////////
	public int run(String[] args) throws Exception {
		Job job1 = new Job(getConf());
		job1.setJarByClass(KJoin.class);
		Path in = new Path(args[0]); // in
		Path out = new Path(args[1]); // out1
		FileInputFormat.addInputPath(job1, in);
		FileOutputFormat.setOutputPath(job1, out);
		job1.setMapperClass(MapClass1.class);
		job1.setReducerClass(Reduce1.class);
		// job1.setCombinerClass( Combine1.class ); // ??
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IntWritable.class);

		// /////////////////////////////////////////////////////////////////////////

		// Job job2 = new Job( getConf() );
		// job2.setJarByClass( KJoin.class );
		// in = new Path( args[1] ); // out1
		// out = new Path( args[2] ); // out2
		// FileInputFormat.addInputPath( job2, in );
		// FileOutputFormat.setOutputPath( job2, out );
		// job2.setMapperClass( MapClass2.class );
		// job2.setReducerClass( Reduce2.class );
		// job2.setCombinerClass( Reduce2.class );
		// job2.setOutputKeyClass( Text.class );
		// job2.setOutputValueClass( IntWritable.class );

		// /////////////////////////////////////////////////////////////////////////

		// job1.setNumReduceTasks( 4 );
		job1.waitForCompletion(true);

		// job2.setNumReduceTasks( 10 );
		// job2.waitForCompletion(true);

		return 0;
	}

	// /////////////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new KJoin(), args);
		System.exit(res);
	}
}