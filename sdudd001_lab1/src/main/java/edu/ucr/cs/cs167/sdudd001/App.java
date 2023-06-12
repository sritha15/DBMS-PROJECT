package edu.ucr.cs.cs167.sdudd001;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class App {

    public static class Revenue_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text monthYear = new Text();

        public void map(LongWritable x, Text y, Context con) throws IOException, InterruptedException {
            String line = y.toString();
            if (line.contains("arrival_year")) return;
            String[] parts = line.split(",");
            double revenue = computeRevenue(parts);
            monthYear.set(parts[0] + "-" + parts[1]);
            con.write(monthYear, new DoubleWritable(revenue));
        }

        private double computeRevenue(String[] parts) {
            Integer booking_status = Integer.parseInt(parts[2]);
            Integer weekend_nights = Integer.parseInt(parts[3]);
            Integer week_nights = Integer.parseInt(parts[4]);
            double revenue = Double.parseDouble(parts[6]);
            return revenue * (weekend_nights + week_nights) * booking_status;
        }
    }

    public static class Revenue_Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text x, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            con.write(x, new DoubleWritable(sum));
        }
    }

    public static class DescendingOrder_Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        public void map(LongWritable x, Text y, Context con) throws IOException, InterruptedException {
            String[] var = y.toString().split("\t");
            if (var.length == 2) {
                double revenue = -1 * Double.parseDouble(var[1]);
                con.write(new DoubleWritable(revenue), new Text(var[0]));
            }
        }
    }

    public static class DescendingOrder_Reducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        public void reduce(DoubleWritable x, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            double rev = -1 * x.get();
            for (Text y : values) {
                con.write(y, new DoubleWritable(rev));
            }
        }
    }

    public static class TopMonthOfYear_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable x, Text y, Context con) throws IOException, InterruptedException {
            String[] var = y.toString().split("\t");
            if (var.length == 2) {
                String[] dateParts = var[0].split("-");
                double revenue = Double.parseDouble(var[1]);
                con.write(new Text(dateParts[0]), new DoubleWritable(revenue));
            }
        }
    }

    public static class TopMonthOfYear_Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text x, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException {
            double maxRevenue = getMaxRevenue(values);
            con.write(x, new DoubleWritable(maxRevenue));
        }

        private double getMaxRevenue(Iterable<DoubleWritable> values) {
            double max = Double.MIN_VALUE;
            for (DoubleWritable y : values) {
                max = Math.max(max, y.get());
            }
            return max;
        }
    }

    public static class TopMonth_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final NavigableMap<Double, String> topMap = new TreeMap<>();

        public void map(LongWritable x, Text y, Context con) {
            String[] var = y.toString().split("\t");
            double revenue = Double.parseDouble(var[1]);
            topMap.put(revenue, var[0]);
            if (topMap.size() > 1) {
                topMap.pollFirstEntry();
            }
        }

        protected void cleanup(Context con) throws IOException, InterruptedException {
            for (Map.Entry<Double, String> entry : topMap.descendingMap().entrySet()) {
                con.write(new Text(entry.getValue()), new DoubleWritable(entry.getKey()));
            }
        }
    }

    public static class Payment_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable x, Text y, Context con) throws IOException, InterruptedException {
            if (x.get() == 0 && y.toString().contains("arrival_year")) return;
            String[] line = y.toString().split(",");
            String month = line[1];
            String method = line[5];
            con.write(new Text(month), new Text(method));
        }
    }

    public static class Payment_Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text x, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            String PopularMethod = computePopularMethod(values);
            con.write(x, new Text(PopularMethod));
        }

        private String computePopularMethod(Iterable<Text> values) {
            Map<String, Integer> c_map = new HashMap<>();
            for (Text y : values) {
                String method = y.toString();
                c_map.merge(method, 1, Integer::sum);
            }

            String PopularMethod = null;
            int Max = 0;
            for (Map.Entry<String, Integer> entry : c_map.entrySet()) {
                if (entry.getValue() > Max) {
                    PopularMethod = entry.getKey();
                    Max = entry.getValue();
                }
            }
            return PopularMethod;
        }
    }

    public static void main(String[] args) throws Exception {
        boolean s1 = setupAndRunJob1(args);
        boolean s2 = setupAndRunJob2(args);
        boolean s3 = setupAndRunJob3(args);
        boolean s4 = setupAndRunJob4(args);
        boolean s5 = setupAndRunJob5(args);

        System.exit(s1 && s2 && s3 && s4 && s5 ? 0 : 1);
    }

    private static boolean setupAndRunJob1(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job j1 = Job.getInstance();
        j1.setJarByClass(App.class);
        j1.setJobName("hotelBookingAnalysis");
        FileInputFormat.addInputPath(j1, new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, new Path(args[1] + "_intermediate"));
        j1.setMapperClass(Revenue_Mapper.class);
        j1.setReducerClass(Revenue_Reducer.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);
        return j1.waitForCompletion(true);
    }

    private static boolean setupAndRunJob2(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job j2 = Job.getInstance();
        j2.setJarByClass(App.class);
        j2.setJobName("SortByRevenue");
        FileInputFormat.addInputPath(j2, new Path(args[1] + "_intermediate"));
        FileOutputFormat.setOutputPath(j2, new Path(args[1]));
        j2.setMapperClass(DescendingOrder_Mapper.class);
        j2.setReducerClass(DescendingOrder_Reducer.class);
        j2.setOutputKeyClass(DoubleWritable.class);
        j2.setOutputValueClass(Text.class);
        return j2.waitForCompletion(true);
    }

    private static boolean setupAndRunJob3(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job j3 = Job.getInstance();
        j3.setJarByClass(App.class);
        j3.setJobName("TopMonthOfYear");
        FileInputFormat.addInputPath(j3, new Path(args[1] + "_intermediate"));
        FileOutputFormat.setOutputPath(j3, new Path(args[1] + "_TopMonthOfYear"));
        j3.setMapperClass(TopMonthOfYear_Mapper.class);
        j3.setReducerClass(TopMonthOfYear_Reducer.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(DoubleWritable.class);
        return j3.waitForCompletion(true);
    }

    private static boolean setupAndRunJob4(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job j4 = Job.getInstance();
        j4.setJarByClass(App.class);
        j4.setJobName("TopMonth");
        FileInputFormat.addInputPath(j4, new Path(args[1] + "_intermediate"));
        FileOutputFormat.setOutputPath(j4, new Path(args[1] + "_TopMonth"));
        j4.setMapperClass(TopMonth_Mapper.class);
        j4.setNumReduceTasks(1);
        j4.setOutputKeyClass(Text.class);
        j4.setOutputValueClass(DoubleWritable.class);
        return j4.waitForCompletion(true);
    }

    private static boolean setupAndRunJob5(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job j5 = Job.getInstance();
        j5.setJarByClass(App.class);
        j5.setJobName("PaymentMethodAnalysis");
        FileInputFormat.addInputPath(j5, new Path(args[0]));
        FileOutputFormat.setOutputPath(j5, new Path(args[1] + "_Payment"));
        j5.setMapperClass(Payment_Mapper.class);
        j5.setReducerClass(Payment_Reducer.class);
        j5.setOutputKeyClass(Text.class);
        j5.setOutputValueClass(Text.class);
        return j5.waitForCompletion(true);
    }
}
