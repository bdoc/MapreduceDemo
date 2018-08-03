package com.num3rd.mapreduce.demo.common.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by pipe on 3/15/17.
 */
public class TextFile2Gzip {
    public static class TokenizerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    public static class IntSumReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(NullWritable.get(), text);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Text file to Gzip");
        job.setJarByClass(TextFile2Gzip.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setCompressOutput(job, true);

        // BZip2Codec, DefaultCodec, GzipCodec
        //FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        //FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}