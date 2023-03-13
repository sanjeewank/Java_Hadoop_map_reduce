package wc;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageTempByRegion {

    public static class TempMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text outKey = new Text();
        private DoubleWritable outValue = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double temperature = 0.0;
        	String region ="none";	
        	
        	Scanner scanner = new Scanner(value.toString());
        	while (scanner.hasNextLine()) {
        	    String line = scanner.nextLine();
        	    String[] tokens = line.toString().split(",");

                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
                Date date = null;
                try {
                    date = dateFormat.parse(tokens[0]);
                } catch (ParseException e) {
                    e.printStackTrace();
                    continue;
                }
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                int year = cal.get(Calendar.YEAR);
                int month = cal.get(Calendar.MONTH) + 1;
              
                try{
                	region = tokens[14];
    				temperature = Double.parseDouble(tokens[10]);
                }catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
                
                
				outKey.set(year + "-" + month + "," + region);
            
				outValue.set(temperature );
                context.write(outKey, outValue);
        	}
        	scanner.close();	
        }
    }

    public static class TempReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable outValue = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }
            double average = sum / count;
            outValue.set(average);
            context.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: AverageTempByRegion <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average temperature by region");

        job.setJarByClass(AverageTempByRegion.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}