package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WikiPageRank
{



    /*
            INGRESSO
                - alfa: per convergenza
                - wiki-micro.txt
                - output
     */
    public static void main(final String[] args) throws Exception {
        System.out.println("*** PageRank Hadoop implementation ***");

        Configuration conf = new Configuration();
        System.out.println("INPUT: "+args[0]+" ,  "+args[1]+"  ,  "+args[2]);
/*
        if (otherArgs.length != 3) {
            System.err.println("Usage: PageRank <#iterations> <input> <output>");
            System.exit(1);
        }

        Path input = new Path(otherArgs[1]);
        Path output = new Path(otherArgs[2]);

        System.out.println("args[0]: <#iterations>=" + otherArgs[0]);
        System.out.println("args[1]: <input>=" + otherArgs[1]);
        System.out.println("args[2]: <output>=" + otherArgs[2]);


        // set number of iterations
        int iterations = Integer.parseInt(otherArgs[0]);

        FileSystem fs = FileSystem.get(output.toUri(),conf);
        if (fs.exists(output)) {
            System.out.println("Delete old output folder: " + output);
            fs.delete(output, true);
        }

        fs = FileSystem.get(new Path(output.toString()+"_initial_ranked").toUri(),conf);
        if (fs.exists(new Path(output.toString()+"_initial_ranked"))) {
            System.out.println("Delete old output folder: " + output.toString()+"_initial_ranked");
            fs.delete(new Path(output.toString()+"_initial_ranked"), true);
        }

        for (int i = 0; i < iterations; i++) {

            System.out.println("Iteration: " + i);
            Job countPages = Job.getInstance(conf, "CountPages");
            countPages.setJarByClass(PageRank.class);

            //se dobbiamo passare qualche altro parametro
            //job.getConfiguration().setInt("", );

            // set mapper/combiner/reducer
            countPages.setMapperClass(CountPagesMapper.class);
            countPages.setCombinerClass(CountPagesReducer.class);
            //job.setPartitionerClass(PageRankPartitioner.class);
            countPages.setReducerClass(CountPagesReducer.class);

            //Da decidere
            countPages.setNumReduceTasks(3);

            // define mapper's output key-value
            countPages.setMapOutputKeyClass(Text.class);
            countPages.setMapOutputValueClass(IntWritable.class);

            // define reducer's output key-value
            countPages.setOutputKeyClass(Text.class);
            countPages.setOutputValueClass(LongWritable.class);

            // define I/O
            FileInputFormat.addInputPath(countPages, input);
            FileOutputFormat.setOutputPath(countPages, output);

            countPages.setInputFormatClass(TextInputFormat.class);
            countPages.setOutputFormatClass(TextOutputFormat.class);

            countPages.waitForCompletion(true);

            long total_pages = countPages.getCounters().findCounter("totalpages_in_wiki", "totalpages_in_wiki").getValue();
            System.out.println("Pages: " + total_pages);

            conf.set("totalpages_in_wiki", String.valueOf(total_pages));

            Job initialRank = Job.getInstance(conf, "InitialRank");
            initialRank.setJarByClass(PageRank.class);

            initialRank.setMapperClass(InitialRankMapper.class);
            initialRank.setCombinerClass(InitialRankReducer.class);
            initialRank.setReducerClass(InitialRankReducer.class);

            initialRank.setNumReduceTasks(3);

            // define mapper's output key-value
            initialRank.setMapOutputKeyClass(Text.class);
            initialRank.setMapOutputValueClass(Text.class);

            // define reducer's output key-value
            initialRank.setOutputKeyClass(Text.class);
            initialRank.setOutputValueClass(Text.class);

            // define I/O
            FileInputFormat.addInputPath(initialRank, input);
            FileOutputFormat.setOutputPath(initialRank, new Path(output + "_initial_ranked"));

            initialRank.setInputFormatClass(TextInputFormat.class);
            initialRank.setOutputFormatClass(TextOutputFormat.class);

            initialRank.waitForCompletion(true);

            //Non ho capito perchè
            fs.delete(output, true);

        System.out.println(":::::::::::::::::::::::::::::::::::::::NUMERO DI PAGINE: "+job.getCounters().findCounter(PageRankInfo.NUMBER_OF_PAGES).getValue());

 */
    }
}