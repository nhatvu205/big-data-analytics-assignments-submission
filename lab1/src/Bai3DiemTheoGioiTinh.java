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

public class Bai3DiemTheoGioiTinh {

    public static class GenderMapper extends Mapper<Object, Text, Text, Text> {
        private final Map<String, String> userGender = new HashMap<>();
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) {
                return;
            }

            Configuration conf = context.getConfiguration();
            for (URI uri : cacheFiles) {
                Path path = new Path(uri.getPath());
                if (!path.getName().equals("users.txt")) {
                    continue;
                }

                FileSystem fs = FileSystem.get(conf);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split(",");
                        if (parts.length >= 2) {
                            userGender.put(parts[0].trim(), parts[1].trim().toUpperCase(Locale.ROOT));
                        }
                    }
                }
            }
        }

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

            String userId = parts[0].trim();
            String movieId = parts[1].trim();
            String ratingStr = parts[2].trim();

            String gender = userGender.get(userId);
            if (gender == null || (!gender.equals("M") && !gender.equals("F"))) {
                return;
            }

            try {
                Double.parseDouble(ratingStr);
            } catch (NumberFormatException e) {
                return;
            }

            outKey.set(movieId);
            outValue.set(gender + ":" + ratingStr);
            context.write(outKey, outValue);
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        private final Map<String, String> movieTitles = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) {
                return;
            }

            Configuration conf = context.getConfiguration();
            for (URI uri : cacheFiles) {
                Path path = new Path(uri.getPath());
                if (!path.getName().equals("movies.txt")) {
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
            double maleSum = 0.0;
            int maleCount = 0;
            double femaleSum = 0.0;
            int femaleCount = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts.length != 2) {
                    continue;
                }

                String gender = parts[0];
                double rating;
                try {
                    rating = Double.parseDouble(parts[1]);
                } catch (NumberFormatException e) {
                    continue;
                }

                if (gender.equals("M")) {
                    maleSum += rating;
                    maleCount++;
                } else if (gender.equals("F")) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            String maleAvg = maleCount > 0
                    ? String.format(Locale.US, "%.2f", maleSum / maleCount)
                    : "N/A";
            String femaleAvg = femaleCount > 0
                    ? String.format(Locale.US, "%.2f", femaleSum / femaleCount)
                    : "N/A";

            String movieId = key.toString();
            String title = movieTitles.getOrDefault(movieId, "MovieID=" + movieId);

            context.write(new Text(title), new Text("Male_Avg: " + maleAvg + ", Female_Avg: " + femaleAvg));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println(
                    "Usage: Bai3DiemTheoGioiTinh <ratings_input_paths(comma-separated)> <users_file_path> <movies_file_path> <output_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Assignment 3 - Movie Rating By Gender");
        job.setJarByClass(Bai3DiemTheoGioiTinh.class);

        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[1]).toUri());
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
