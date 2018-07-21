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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AllMovies {
    static Set<String> movieSet = new HashSet<String>();
    public static class MoviesMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //value = userid \t movie1: rating, movie2: rating...
            //key = movie1: movie2 value = 1
            String line = value.toString().trim();
            String[] user_movieRatings = line.split("，");
            String movie = user_movieRatings[1];
            if(!movieSet.contains(movie)) {
                movieSet.add(movie);
                context.write(new Text("movies"), new Text(movie));
            }
        }
    }

    public static class MoviesReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //key movie1:movie2 value = iterable<1, 1, 1>

            while(values.iterator().hasNext()) {
                String movie = values.iterator().next().toString();
                context.write(new Text(movie), new Text(""));
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(AllMovies.MoviesMapper.class);
        job.setReducerClass(AllMovies.MoviesReducer.class);

        job.setJarByClass(AllMovies.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[7]));

        job.waitForCompletion(true);

    }
}
