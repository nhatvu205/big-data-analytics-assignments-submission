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

public class Bai4DiemTheoNhomTuoi {

    public static class AgeGroupMapper extends Mapper<Object, Text, Text, Text> {
        private final Map<String, Integer> userAge = new HashMap<>();
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
                        if (parts.length >= 3) {
                            try {
                                userAge.put(parts[0].trim(), Integer.parseInt(parts[2].trim()));
                            } catch (NumberFormatException e) {
                                // Skip invalid age.
                            }
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

            Integer age = userAge.get(userId);
            if (age == null) {
                return;
            }

            try {
                Double.parseDouble(ratingStr);
            } catch (NumberFormatException e) {
                return;
            }

            String group = toAgeGroup(age);
            outKey.set(movieId);
            outValue.set(group + ":" + ratingStr);
            context.write(outKey, outValue);
        }

        private String toAgeGroup(int age) {
            if (age <= 18) {
                return "0-18";
            }
            if (age <= 35) {
                return "18-35";
            }
            if (age <= 50) {
                return "35-50";
            }
            return "50+";
        }
    }

    public static class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {
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
            double sum0to18 = 0.0;
            int count0to18 = 0;
            double sum18to35 = 0.0;
            int count18to35 = 0;
            double sum35to50 = 0.0;
            int count35to50 = 0;
            double sum50plus = 0.0;
            int count50plus = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts.length != 2) {
                    continue;
                }

                String group = parts[0];
                double rating;
                try {
                    rating = Double.parseDouble(parts[1]);
                } catch (NumberFormatException e) {
                    continue;
                }

                switch (group) {
                    case "0-18":
                        sum0to18 += rating;
                        count0to18++;
                        break;
                    case "18-35":
                        sum18to35 += rating;
                        count18to35++;
                        break;
                    case "35-50":
                        sum35to50 += rating;
                        count35to50++;
                        break;
                    case "50+":
                        sum50plus += rating;
                        count50plus++;
                        break;
                    default:
                        break;
                }
            }

            String avg0to18 = count0to18 > 0 ? String.format(Locale.US, "%.2f", sum0to18 / count0to18) : "N/A";
            String avg18to35 = count18to35 > 0 ? String.format(Locale.US, "%.2f", sum18to35 / count18to35) : "N/A";
            String avg35to50 = count35to50 > 0 ? String.format(Locale.US, "%.2f", sum35to50 / count35to50) : "N/A";
            String avg50plus = count50plus > 0 ? String.format(Locale.US, "%.2f", sum50plus / count50plus) : "N/A";

            String movieId = key.toString();
            String title = movieTitles.getOrDefault(movieId, "MovieID=" + movieId);

            context.write(
                    new Text(title),
                    new Text("[0-18: " + avg0to18
                            + ", 18-35: " + avg18to35
                            + ", 35-50: " + avg35to50
                            + ", 50+: " + avg50plus + "]"));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println(
                    "Usage: Bai4DiemTheoNhomTuoi <ratings_input_paths(comma-separated)> <users_file_path> <movies_file_path> <output_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Assignment 4 - Movie Rating By Age Group");
        job.setJarByClass(Bai4DiemTheoNhomTuoi.class);

        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);

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
