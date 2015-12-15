import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class RocketReducer {

//    public static class TokenizerMapper
//            extends Mapper<Object, Text, Text, LongWritable>{
//
//        private final static LongWritable one = new LongWritable(1);
//        private Text word = new Text();
//
//        public void map(Object key, Text value, Context context
//        ) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString());
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                context.write(word, one);
//            }
//        }
//    }

    public static class ArticleMapper extends Mapper<Object,Text,Text,LongWritable>{
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();
        String pattern = "<(article|inproceedings|phdthesis|masterthesis)";
        Pattern r = Pattern.compile(pattern);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Matcher m = r.matcher(value.toString());
            while (m.find()) {
                word.set(m.group(1));
                context.write(word,one);
            }
        }
    }

    public static class Top5Mapper extends Mapper<Object,Text,Text,LongWritable>{
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();
        String pattern = "<author>(.*)</author>";
        Pattern r = Pattern.compile(pattern);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Matcher m = r.matcher(value.toString());
            while (m.find()) {
                word.set(m.group(1));
                context.write(word,one);
            }
        }
    }

    public static class LongSumReducer
            extends Reducer<Text,LongWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Path tempDir =
                new Path("rocket-article"+
                        Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Path tempDir2 =
                new Path("rocket-top5"+
                        Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Path tempDir3 =
                new Path("rocket-sort"+
                        Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "rocket reducer phase 1");
        Job top5job = Job.getInstance(conf, "rocket reducer phase 2");
        Job sortjob = Job.getInstance(conf, "rocket reducer phase 3");
        Job invjob = Job.getInstance(conf, "rocket reducer phase 4");
        try {
            top5job.setJarByClass(RocketReducer.class);
            top5job.setMapperClass(Top5Mapper.class);
            top5job.setCombinerClass(LongSumReducer.class);
            top5job.setReducerClass(LongSumReducer.class);
            top5job.setOutputFormatClass(SequenceFileOutputFormat.class);
            //top5job.setSortComparatorClass(LongWritable.Comparator.class);
            top5job.setOutputKeyClass(Text.class);
            top5job.setOutputValueClass(LongWritable.class);
            FileInputFormat.addInputPath(top5job, new Path(args[0]));
            FileOutputFormat.setOutputPath(top5job, tempDir2);
            top5job.waitForCompletion(true);
            sortjob.setJarByClass(RocketReducer.class);
            sortjob.setMapperClass(InverseMapper.class);
            sortjob.setInputFormatClass(SequenceFileInputFormat.class);
//            sortjob.setOutputFormatClass(SequenceFileOutputFormat.class);
            sortjob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            FileInputFormat.addInputPath(sortjob, tempDir2);
            FileOutputFormat.setOutputPath(sortjob, new Path(args[1]));
            sortjob.waitForCompletion(true);
//            invjob.setJarByClass(RocketReducer.class);
//            invjob.setMapperClass(InverseMapper.class);
//            invjob.setInputFormatClass(SequenceFileInputFormat.class);
//            FileInputFormat.addInputPath(invjob, tempDir3);
//            FileOutputFormat.setOutputPath(invjob, new Path(args[1]));
//            invjob.waitForCompletion(true);



//            Job sortJob = Job.getInstance(conf);
//            sortJob.setJobName("grep-sort");
//            sortJob.setJarByClass(RocketReducer.class);
//            FileInputFormat.addInputPath(sortJob, tempDir);
//            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
//
//            sortJob.setMapperClass(InverseMapper.class);
//
//            sortJob.setNumReduceTasks(1);
//            FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
//            sortJob.setSortComparatorClass(          // sort by decreasing freq
//                    IntWritable.DecreasingComparator.class);
//            sortJob.waitForCompletion(true);
        }
        finally {
            FileSystem.get(conf).delete(tempDir, true);
        }

        System.exit(0);
    }
}