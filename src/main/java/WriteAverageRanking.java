import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class WriteAverageRanking {
    static Map<String, Map<String, String>> rankedMovie = new HashMap<String, Map<String, String>>();

    public WriteAverageRanking(Map<String, Map<String, String>> rankedMovie) {
        this.rankedMovie = rankedMovie;
    }

    public static class MoviesMapper extends Mapper<LongWritable, Text, Text, Text> {
        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //value = userid \t movie1: rating, movie2: rating...
            //key = movie1: movie2 value = 1
            String line = value.toString().trim();
            String[] movie_space = line.split("\t");
            String movie = movie_space[0];
            for(Map.Entry<String, Map<String, String>> entry : rankedMovie.entrySet()) {
                if(!entry.getValue().containsKey(movie)) {
                   context.write(new Text(entry.getKey()), new Text("movie:"+ movie));
                }
            }
        }
    }

    public static class ReadAverageMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] user_average = line.split("\t");
            context.write(new Text(user_average[0]), new Text(user_average[1]));

        }
    }

    public static class WriteReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //key movie1:movie2 value = iterable<1, 1, 1>
            List<String> unwatchedList = new ArrayList<String>();
            double average = 0;
            for(Text text : values) {
                String str = text.toString();
                if(str.contains(":")) {
                    String[] arr = str.split(":");
                    unwatchedList.add(arr[1]);
                } else {
                    average = Double.parseDouble(str);
                }
            }

            for (String movie : unwatchedList) {
                context.write(new Text(key), new Text(movie + ":" + average));
            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(WriteAverageRanking.class);

        ChainMapper.addMapper(job, WriteAverageRanking.MoviesMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, WriteAverageRanking.ReadAverageMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(WriteAverageRanking.MoviesMapper.class);
        job.setMapperClass(WriteAverageRanking.ReadAverageMapper.class);

        job.setReducerClass(WriteAverageRanking.WriteReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, WriteAverageRanking.MoviesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, WriteAverageRanking.ReadAverageMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}
