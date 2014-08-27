/************************************************************/
/** Parallel Set Similarity Join in MapReduce ( Vernica ) ***/
/** Author: Kaeser Md. Sabrin *******************************/
/** Date: 1st-7th November, 2012 ****************************/
/** Hadoop Version 0.20.2 ***********************************/
/************************************************************/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import javax.security.auth.login.Configuration;
import javax.tools.Tool;

import com.sun.xml.internal.bind.CycleRecoverable.Context;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Text;

public class SSJoin extends Configured implements Tool {
	/*********************************************************************************/
	/******** Mapper and Reducer classes for Phase 0 *********************************/
	/******** Input: Text files in <textid, text> format *****************************/
	/******** Output: Text files in <same> format ************************************/
	/******** To evenly distribute the data among all the nodes **********************/
	/*********************************************************************************/

	public static class MapClass0 extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}

	/*-------------------------------------------------------------------------------*/

	public static class Reduce0 extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	/*********************************************************************************/
	/******** Mapper and Reducer classes for Phase 1 *********************************/
	/******** Input: Text files in <textid, text> format *****************************/
	/******** Output: Text files in <token, frequency> format ************************/
	/*********************************************************************************/

	public static class MapClass1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1); // constant
																	// counter
		private Text word = new Text();
		private static Set<String> stopWords = new HashSet<String>(20);

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
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());

			itr.nextToken(); // skip the textid

			while (itr.hasMoreTokens()) {
				String s = itr.nextToken();
				if (s.length() > 1 && stopWords.contains(s) == false) {
					word.set(s);
					context.write(word, one);
				}
			}
		}
	}

	/*-------------------------------------------------------------------------------*/

	public static class Reduce1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable sum = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int s = 0;
			for (IntWritable val : values) {
				s += val.get();
			}

			sum.set(s);
			context.write(key, sum);
		}
	}

	/*********************************************************************************/
	/******** Mapper and Reducer classes for Phase 2 *********************************/
	/******** Input: Text files in <token, frequencey> format ************************/
	/******** Output: Text files in <sorted_frequency, token> format *****************/
	/*********************************************************************************/

	public static class MapClass2 extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable freq = new IntWritable();
		private Text term = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			term.set(itr.nextToken());
			freq.set(Integer.parseInt(itr.nextToken()));

			context.write(freq, term);
		}
	}

	/*-------------------------------------------------------------------------------*/

	public static class Reduce2 extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(key, val);
			}
		}
	}

	/*********************************************************************************/
	/******** Mapper and Reducer classes for Phase 3 *********************************/
	/******** Input 1: Text files in <token, frequency> format ***********************/
	/******** Input 2: Global Token Frequency to all mappers from phase 2 ************/
	/******** Output: Text files in <textid1, textid2, similarity> format ************/
	/*********************************************************************************/

	public static class MapClass3 extends
			Mapper<LongWritable, Text, Text, Text> {
		private static HashMap<String, Integer> hmap = new HashMap<String, Integer>();
		private final static double simThld = 0.9;
		private Text prefixWord = new Text();

		public void setup(Context context) {
			/*
			 * // Global frequency sorted token input from Distributed Cache try
			 * { URI [] cacheFiles = DistributedCache.getCacheFiles(
			 * context.getConfiguration() );
			 * 
			 * 
			 * if ( cacheFiles != null && cacheFiles.length > 0 ) {
			 * 
			 * System.out.println( "File Found: " ); for ( URI cachePath :
			 * cacheFiles ) { System.out.println( cachePath.getPath() );
			 * 
			 * 
			 * if ( cachePath.getPath().equals( "/user/hduser/out2/part-r-00000"
			 * ) ) { System.out.println( "!!!!!" );
			 * 
			 * BufferedReader wordReader = new BufferedReader(new
			 * FileReader(cachePath.toString()));
			 * 
			 * String s = null; while ( ( s = wordReader.readLine() ) != null )
			 * { StringTokenizer itr = new StringTokenizer( s ); int freq =
			 * Integer.parseInt( itr.nextToken() ); String word =
			 * itr.nextToken(); hmap.put( word, freq ); System.out.println( word
			 * + "\t" + freq ); }
			 * 
			 * wordReader.close(); break; } } } } catch (IOException e) {}
			 */

			try {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				FileStatus[] status = fs.listStatus(new Path("out2"));
				for (int i = 0; i < status.length; ++i) {
					String fname = status[i].getPath().getName();
					if (fname.charAt(0) != 'p')
						continue; // file name must be part-r-xxxxx
					BufferedReader d = new BufferedReader(
							new InputStreamReader(fs.open(status[i].getPath())));

					String s = null;
					while ((s = d.readLine()) != null) {
						StringTokenizer itr = new StringTokenizer(s);
						int freq = Integer.parseInt(itr.nextToken());
						String word = itr.nextToken();
						hmap.put(word, freq);
						// System.out.println( word + "\t"+ freq );
					}
					d.close();
				}
			} catch (IOException e) {
			}

		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("[ \t\r\n]+");

			tokens[0] = "of"; // removing the text-id
			Arrays.sort(tokens, new Comparator<String>() {
				public int compare(String s1, String s2) {
					int a = 10000000, b = 10000000;
					if (hmap.containsKey(s1))
						a = hmap.get(s1);
					if (hmap.containsKey(s2))
						b = hmap.get(s2);
					return a - b;
				}
			});

			int prefixLength = tokens.length - (int) (simThld * tokens.length); //

			for (int i = 0; i < tokens.length && i < prefixLength; ++i) {
				prefixWord.set(tokens[i]);
				context.write(prefixWord, value);
			}
		}
	}

	/*-------------------------------------------------------------------------------*/

	public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
		private final static double simThld = 0.9;
		private Text pair = new Text();
		private Text siml = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String[]> slist = new ArrayList<String[]>();
			// System.out.println( key.toString() );
			for (Text val : values) {
				String[] v = val.toString().split("[ \t\r\n]+");
				slist.add(v);
				// System.out.println( "\t" + val.toString() );
			}

			// heuristic stop words removal
			if (slist.size() > 200)
				return;

			for (int i = 0; i < slist.size(); ++i) {
				String[] v1 = slist.get(i);
				for (int j = 0; j < slist.size(); ++j) {
					String[] v2 = slist.get(j);

					if (Integer.parseInt(v1[0]) >= Integer.parseInt(v2[0]))
						continue;
					double l1 = v1.length;
					double l2 = v2.length;
					if ((l1 <= l2) && (l2 <= (1 / (simThld * l1))))
						continue;

					int cap = 0; // cap is for intersection

					if (l1 < l2) {
						HashSet<String> hset = new HashSet<String>(
								Arrays.asList(v2));
						for (int k = 1; k < v1.length; ++k)
							if (hset.contains(v1[k]) == true)
								++cap;
						hset.clear();
					} else {
						HashSet<String> hset = new HashSet<String>(
								Arrays.asList(v1));
						for (int k = 1; k < v2.length; ++k)
							if (hset.contains(v2[k]) == true)
								++cap;
						hset.clear();
					}

					double sim = (double) cap
							/ (double) (v1.length + v2.length - cap - 2);

					// System.out.println( v1[0] + "," + v2[0] + "=" + sim +
					// " for " + key.toString() );

					if (sim * 100 > simThld * 100) {
						pair.set(new String(v1[0] + "," + v2[0]));
						siml.set(new Double(sim * 100).toString());
						context.write(pair, siml);
					}
				}
			}

			slist.clear();
		}
	}

	/**********************************************************************************/
	// /////////////////////////////////
	// usage in out1 out2 out3 ////////
	// /////////////////////////////////
	public int run(String[] args) throws Exception {
		// /////////////////////////////////////////////////////////////////////////
		/*
		 * Job job0 = new Job( getConf() ); job0.setJarByClass( SSJoin.class );
		 * Path in = new Path( args[0] ); // in Path out = new Path( args[1] );
		 * // out0 FileInputFormat.addInputPath(job0, in);
		 * FileOutputFormat.setOutputPath(job0, out); job0.setMapperClass(
		 * MapClass0.class ); job0.setReducerClass( Reduce0.class );
		 * job0.setOutputKeyClass( Text.class ); job0.setOutputValueClass(
		 * NullWritable.class );
		 */
		// /////////////////////////////////////////////////////////////////////////

		Job job1 = new Job(getConf());
		job1.setJarByClass(SSJoin.class);
		Path in = new Path(args[0]); // in
		Path out = new Path(args[1]); // out1
		FileInputFormat.addInputPath(job1, in);
		FileOutputFormat.setOutputPath(job1, out);
		job1.setMapperClass(MapClass1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setCombinerClass(Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		// /////////////////////////////////////////////////////////////////////////

		Job job2 = new Job(getConf());
		job2.setJarByClass(SSJoin.class);
		in = new Path(args[1]); // out1
		out = new Path(args[2]); // out2
		FileInputFormat.addInputPath(job2, in);
		FileOutputFormat.setOutputPath(job2, out);
		job2.setMapperClass(MapClass2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setCombinerClass(Reduce2.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);

		// /////////////////////////////////////////////////////////////////////////

		Job job3 = new Job(getConf());
		job3.setJarByClass(SSJoin.class);
		in = new Path(args[0]); // in
		out = new Path(args[3]); // out3
		FileInputFormat.addInputPath(job3, in);
		FileOutputFormat.setOutputPath(job3, out);
		job3.setMapperClass(MapClass3.class);
		job3.setReducerClass(Reduce3.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		// /////////////////////////////////////////////////////////////////////////

		// job0.setNumReduceTasks(40);
		// job0.waitForCompletion(true);

		// job1.setNumReduceTasks(20);
		job1.waitForCompletion(true);

		job2.setNumReduceTasks(1);
		job2.waitForCompletion(true);

		// ///////////////DISTRIBUTED CACHE NOT WORKING !!!!!!
		// //////////////////////
		// DistributedCache.addCacheFile( new
		// URI("/user/hduser/out2/part-r-00000"), getConf() );
		// URI [] cacheFiles = DistributedCache.getCacheFiles( getConf() );
		// if ( cacheFiles != null ) System.out.println( "ADDED " +
		// cacheFiles[0].getPath() );
		// else System.out.println( "Not Added" );
		// //////////////////////////////////////////////////////////////////////////

		// job3.setNumReduceTasks(20);
		job3.waitForCompletion(true);

		return 0;
	}

	// /////////////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SSJoin(), args);
		System.exit(res);
	}
}
