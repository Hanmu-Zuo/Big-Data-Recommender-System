import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataDividerByUser {
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input user,movie,rating
			String[] user_movie_rating = value.toString().trim().split(",");
			int userID = Integer.parseInt(user_movie_rating[0]);
			String movieID = user_movie_rating[1];
			String rating = user_movie_rating[2];

			context.write(new IntWritable(userID), new Text(movieID + ":" + rating));
		}
	}


	public static class AverageRankingDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input user \t movie:rating
			String[] user_movie_rating = value.toString().trim().split("\t");
			int userID = Integer.parseInt(user_movie_rating[0]);
			String[] movie_rating = user_movie_rating[1].split(":");
			String movieID = movie_rating[0];
			String rating = movie_rating[1];
			context.write(new IntWritable(userID), new Text(movieID + ":" + rating));
		}
	}

	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			while (values.iterator().hasNext()) {
				sb.append("," + values.iterator().next());
			}
			//key = user value=movie1:rating, movie2:rating...
			context.write(key, new Text(sb.toString().replaceFirst(",", "")));
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(DataDividerByUser.class);

		ChainMapper.addMapper(job, DataDividerByUser.DataDividerMapper.class, LongWritable.class, Text.class, IntWritable.class, Text.class, conf);
		ChainMapper.addMapper(job, DataDividerByUser.AverageRankingDividerMapper.class, Text.class, Text.class, IntWritable.class, Text.class, conf);

		job.setMapperClass(DataDividerByUser.DataDividerMapper.class);
		job.setMapperClass(DataDividerByUser.AverageRankingDividerMapper.class);

		job.setReducerClass(DataDividerByUser.DataDividerReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DataDividerByUser.DataDividerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DataDividerByUser.AverageRankingDividerMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}


}
