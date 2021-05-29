package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import it.unipi.hadoop.dataModel.CustomCounter;
import it.unipi.hadoop.dataModel.CustomOutputFormat;
import it.unipi.hadoop.dataModel.Node;
import it.unipi.hadoop.job.NPagesAndOutlinks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WikiPageRank
{



    /*
            INGRESSO
                - alfa: damping factor args[0]
                - threshold: soglia per la convergenza args[1]
                - wiki-micro.txt args[2]
                - output args[3]
     */
    public static void main(final String[] args) throws Exception {
        System.out.println("*** PageRank Hadoop implementation ***");

        Configuration conf = new Configuration();
        System.out.println("INPUT: "+args[0]+" ,  "+args[1]+"  ,  "+args[2]+"  ,  "+args[3]);

        if (args.length != 4) {
            System.err.println("Usage: PageRank <#alfa> <input> <output>");
            System.exit(1);
        }

        //set from inputs
        double dampingFactor = Double.parseDouble(args[0]);
        double threshold = Double.parseDouble(args[1]);
        Path input = new Path(args[2]);
        Path output = new Path(args[3]);

        //::::::::::::::::::::::::::::::first JOB: compute number of pages and outlinks:::::::::::::::::::::::::::::::::::::::::::::::::::::::::

        Job nPagesAndOutlinks_job = Job.getInstance(conf);
        nPagesAndOutlinks_job.setJarByClass(WikiPageRank.class);
        nPagesAndOutlinks_job.setJobName("Compute number of pages and outlinks ");

        FileInputFormat.addInputPath(nPagesAndOutlinks_job, input);
        FileOutputFormat.setOutputPath(nPagesAndOutlinks_job, new Path(output+"/firstJob"));

        nPagesAndOutlinks_job.setNumReduceTasks(0);

        nPagesAndOutlinks_job.setMapperClass(NPagesAndOutlinks.NPagesAndOutlinksMapper.class);

        nPagesAndOutlinks_job.setMapOutputKeyClass(Text.class);
        nPagesAndOutlinks_job.setMapOutputValueClass(Node.class);

        nPagesAndOutlinks_job.setInputFormatClass(TextInputFormat.class);
        nPagesAndOutlinks_job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //wait
        boolean success = nPagesAndOutlinks_job.waitForCompletion(true);
        if(success)
            System.out.println("Lavoro completato");
        else {
            System.out.println("Lavoro fallito: non è stato possibile terminare il conteggio del numero delle pagine e dei rispettivi outlinks");
            System.exit(0);
        }

        //add field into xml configuration file (we will use that in other map reduce taks)
        long numberOfPages = nPagesAndOutlinks_job.getCounters().findCounter(CustomCounter.NUMBER_OF_PAGES).getValue();
        conf.set("number_of_pages",String.valueOf(numberOfPages));
/*
        //::::::::::::::::::::::::::::::second JOB: compute final rank until convergence:::::::::::::::::::::::::::::::::::::::::::::::::::::::::

        Job computePageRank_job = Job.getInstance(conf);
        computePageRank_job.setJarByClass(WikiPageRank.class);
        computePageRank_job.setJobName("Compute page rank ");

        //as input take previous result
        FileInputFormat.addInputPath(computePageRank_job, new Path(output+"/firstJob"));
        FileOutputFormat.setOutputPath(computePageRank_job, new Path(output+"/secondJob"));

        //computePageRank_job.setNumReduceTasks(3);

        computePageRank_job.setMapperClass(NPagesAndOutlinks.NPagesAndOutlinksMapper.class);
        //nPagesAndOutlinks_job.setReducerClass(NPagesAndOutlinks.NPagesAndOutlinksReducer.class);

        nPagesAndOutlinks_job.setMapOutputKeyClass(Text.class);
        nPagesAndOutlinks_job.setMapOutputValueClass(Text.class);
        //nPagesAndOutlinks_job.setOutputKeyClass(Text.class);
        //nPagesAndOutlinks_job.setOutputValueClass(IntWritable.class);

        nPagesAndOutlinks_job.setInputFormatClass(TextInputFormat.class);
        nPagesAndOutlinks_job.setOutputFormatClass(TextOutputFormat.class);

        //wait
        boolean succes = nPagesAndOutlinks_job.waitForCompletion(true);
        if(success)
            System.out.println("Lavoro completato");
        else {
            System.out.println("Lavoro fallito: non è stato possibile terminare il conteggio del numero delle pagine e dei rispettivi outlinks");
            System.exit(0);
        }


*/




        /*

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
*/
        System.out.println(":::::::::::::::::::::::::::::::::::::::NUMERO DI PAGINE: "+nPagesAndOutlinks_job.getCounters().findCounter(CustomCounter.NUMBER_OF_PAGES).getValue());

        System.exit(1);


    }
}