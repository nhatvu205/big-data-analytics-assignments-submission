import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai1DiemTBPhim {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");
            if (parts.length < 3) {
                return;
            }

            String movieId = parts[1].trim();
            String ratingStr = parts[2].trim();

            try {
                Double.parseDouble(ratingStr);
            } catch (NumberFormatException e) {
                return;
            }

            outKey.set(movieId);
            outValue.set(ratingStr);
            context.write(outKey, outValue);
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private final Map<String, String> movieTitles = new HashMap<>();

        private String maxMovie = "N/A";
        private double maxRating = -1.0;

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) {
                return;
            }

            Configuration conf = context.getConfiguration();
            for (URI uri : cacheFiles) {
                Path path = new Path(uri.getPath());
                String name = path.getName();
                if (!name.equals("movies.txt")) {
                    continue;
                }

                FileSystem fs = FileSystem.get(conf);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split(",", 3);
                        if (parts.length >= 2) {
                            movieTitles.put(parts[0].trim(), parts[1].trim());
                        }
                    }
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for (Text value : values) {
                try {
                    sum += Double.parseDouble(value.toString());
                    count++;
                } catch (NumberFormatException e) {
                    // Skip invalid rating.
                }
            }

            if (count == 0) {
                return;
            }

            double avg = sum / count;
            String movieId = key.toString();
            String movieTitle = movieTitles.getOrDefault(movieId, "MovieID=" + movieId);

            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = movieTitle;
            }

            context.write(
                    new Text(movieTitle),
                    new Text(String.format(Locale.US, "AverageRating: %.2f (TotalRatings: %d)", avg, count)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (maxRating >= 0.0) {
                context.write(
                        new Text("SUMMARY"),
                        new Text(String.format(
                                Locale.US,
                                "%s is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.",
                                maxMovie,
                                maxRating)));
            } else {
                context.write(
                        new Text("SUMMARY"),
                        new Text("No movie has at least 5 ratings to determine the highest rated movie."));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println(
                    "Usage: Bai1DiemTBPhim <ratings_input_paths(comma-separated)> <movies_file_path> <output_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Assignment 1 - Movie Rating Stats");
        job.setJarByClass(Bai1DiemTBPhim.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[1]).toUri());

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
