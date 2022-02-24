import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StartLetter {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                String woord = itr.nextToken();
                woord = woord.toLowerCase();
                char firstLetter = woord.charAt(0);
                
                // negeer leestekens, cijfers, etc.
                if(Character.isLetter(firstLetter)){
                    //word.set(firstLetter);
                    word.set(Character.toString(firstLetter));
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val: values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count java");
        // waar staat de map en reduce code
        job.setJarByClass(StartLetter.class);
        
        // configure mapper
        job.setMapperClass(TokenizerMapper.class);
        
        //configure reducer
        job.setReducerClass(IntSumReducer.class);
        
        // configure input/output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // input file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // output file
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // start de applicatie
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
