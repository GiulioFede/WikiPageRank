package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import it.unipi.hadoop.dataModel.CustomCounter;
import it.unipi.hadoop.dataModel.Node;
import it.unipi.hadoop.job.NPagesAndOutlinks;
import it.unipi.hadoop.job.PageRank;
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
                - damping factor: damping factor args[0]
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
        nPagesAndOutlinks_job.setOutputFormatClass(TextOutputFormat.class);

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

        //::::::::::::::::::::::::::::::second JOB: compute final rank until convergence:::::::::::::::::::::::::::::::::::::::::::::::::::::::::

        conf.set("damping_factor",String.valueOf(dampingFactor));

        for(int i=0; i<2; i++) {

            Job computePageRank_job = Job.getInstance(conf);
            computePageRank_job.setJarByClass(WikiPageRank.class);
            computePageRank_job.setJobName("Compute page rank ");

            //as input take previous result
            if(i==0)
                FileInputFormat.addInputPath(computePageRank_job, new Path(output + "/firstJob"));
            else
                FileInputFormat.addInputPath(computePageRank_job, new Path(output + "/secondJob_"+(i-1)));

            FileOutputFormat.setOutputPath(computePageRank_job, new Path(output + "/secondJob_"+i));

            computePageRank_job.setNumReduceTasks(1);

            computePageRank_job.setMapperClass(PageRank.PageRankMapper.class);
            computePageRank_job.setReducerClass(PageRank.PageRankReducer.class);

            computePageRank_job.setMapOutputKeyClass(Text.class);
            computePageRank_job.setMapOutputValueClass(Node.class);
            computePageRank_job.setOutputKeyClass(Text.class);
            computePageRank_job.setOutputValueClass(Node.class);

            computePageRank_job.setInputFormatClass(TextInputFormat.class);
            computePageRank_job.setOutputFormatClass(TextOutputFormat.class);

            //wait
            success = computePageRank_job.waitForCompletion(true);
            if (success)
                System.out.println("Lavoro completato");
            else {
                System.out.println("Lavoro fallito: non è stato possibile calcolare il page rank");
                System.exit(0);
            }

        }

        System.exit(1);


    }
}