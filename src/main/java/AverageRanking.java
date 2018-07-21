import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.glassfish.grizzly.compression.lzma.impl.lz.InWindow;

public class AverageRanking {
    static Map<String, Map<String, String>> rankedMovie = new HashMap<String, Map<String, String>>();
    public static class ListGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //value = userid \t movie1: rating, movie2: rating...
            //key = movie1: movie2 value = 1
            String line = value.toString().trim();
            String[] user_movieRatings = line.split("，");
            String movie_rating = user_movieRatings[2];
            String movie = user_movieRatings[1];
            String user = user_movieRatings[0];
            if(!rankedMovie.containsKey(user)) {
                Map<String, String> movieRanking = new HashMap<String, String>();
                movieRanking.put(movie, movie_rating);
                rankedMovie.put(user, movieRanking);
            } else {
                rankedMovie.get(user).put(movie, movie_rating);
            }
            int movie_ranking = Integer.parseInt(movie_rating);
            context.write(new Text(user), new IntWritable(movie_ranking));

        }
    }

    public static class ListGeneratorReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //key movie1:movie2 value = iterable<1, 1, 1>
            int sum = 0;
            int count = 0;
            while(values.iterator().hasNext()) {
                sum += values.iterator().next().get();
                count ++;
            }

            context.write(key, new DoubleWritable(sum / count));
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(AverageRanking.ListGeneratorMapper.class);
        job.setReducerClass(AverageRanking.ListGeneratorReducer.class);

        job.setJarByClass(AverageRanking.class);

        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[6]));

        job.waitForCompletion(true);

    }
}
