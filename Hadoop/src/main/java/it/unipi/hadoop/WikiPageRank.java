package it.unipi.hadoop;

import it.unipi.hadoop.dataModel.CustomCounter;
import it.unipi.hadoop.dataModel.Node;
import it.unipi.hadoop.job.NPagesAndOutlinks;
import it.unipi.hadoop.job.PageRank;
import it.unipi.hadoop.job.RankSort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WikiPageRank
{
    /** INPUT
     *  - damping factor: args[0]
     *  - wiki-micro.txt args[1]
     *  - output args[2]
     */

    public static void main(final String[] args) throws Exception {
        System.out.println(":::::::::::::::::::::::::PageRank Hadoop implementation::::::::::::::::::::::::: ");

        Configuration conf = new Configuration();

        if (args.length != 3) {
            System.err.println("Usage: PageRank <#alfa> <input> <output>");
            System.exit(1);
        }

        //get damping factor from input
        double dampingFactor = Double.parseDouble(args[0]);
        //get 'input path' from input
        Path input = new Path(args[1]);
        //get 'output path' from input --> NB: this output path will be used as folder for storing all partial results including the final rank
        Path output = new Path(args[2]);

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
            System.out.println("Job completed successfully: parsing and number of pages completed");
        else {
            System.out.println("Job failed: it was not possible to parse and finish counting the number of pages and respective outlinks");
            System.exit(0);
        }

        //add field into xml configuration file (we will use that in other map reduce tasks)
        long numberOfPages = nPagesAndOutlinks_job.getCounters().findCounter(CustomCounter.NUMBER_OF_PAGES).getValue();
        conf.set("number_of_pages",String.valueOf(numberOfPages));

        //::::::::::::::::::::::::::::::second JOB: compute final rank iteratively:::::::::::::::::::::::::::::::::::::::::::::::::::::::::

        //add field into xml configuration file (we will use that in each iteration of this second job)
        conf.set("damping_factor",String.valueOf(dampingFactor));



        int i;
        for(i=0; i<10; i++) {

            Job computePageRank_job = Job.getInstance(conf);
            computePageRank_job.setJarByClass(WikiPageRank.class);
            computePageRank_job.setJobName("Compute page rank");
            computePageRank_job.getConfiguration().set("convergence", String.valueOf(0));

            /*
                we have to discriminate the first iteration from the remaining ones because in the first iteration the input
                we use comes from the output of the first job, while the following inputs come from the outputs of the second iterative jobs
             */
            if(i==0)
                FileInputFormat.addInputPath(computePageRank_job, new Path(output + "/firstJob"));
            else
                FileInputFormat.addInputPath(computePageRank_job, new Path(output + "/secondJob_"+(i-1)));

            FileOutputFormat.setOutputPath(computePageRank_job, new Path(output + "/secondJob_"+i));

            computePageRank_job.setNumReduceTasks(3);

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
            if (success) {
                System.out.println("Job 2 (iteration " + (i + 1) + ") completed successfully");
            }
            else {
                System.out.println("Job failed: page rank could not be calculated");
                System.exit(0);
            }
/*
            if(Integer.parseInt(computePageRank_job.getConfiguration().get("convergence")) == 0){
                    System.out.println("Convergence is achieved after "+ (i+1)+" iterations" +
                            " Value:" +nPagesAndOutlinks_job.getCounters().findCounter(CustomCounter.CONVERGENCE).getValue());
                    i++;
                    break;
                }
*/

        }

        //:::::::::::::::::::::::::::::::::: third job: compute sorting ::::::::::::::::::::::::::::::::::::::::::::::::

        Job computeSort_job = Job.getInstance(conf);
        computeSort_job.setJarByClass(WikiPageRank.class);
        computeSort_job.setJobName("Compute sorting ");


        //as input take last result of second job
        FileInputFormat.addInputPath(computeSort_job, new Path(output + "/secondJob_"+(i-1)));
        FileOutputFormat.setOutputPath(computeSort_job, new Path(output + "/finalPageRank"));

        /*
        We use only one reducer because if we used several reducers we will have that each perform a local sorting
        of the keys it receives preventing us from recovering the entire ranking by simply merging the outputs of each reducer
         */
        computeSort_job.setNumReduceTasks(1);

        computeSort_job.setMapperClass(RankSort.RankSortMapper.class);
        computeSort_job.setReducerClass(RankSort.RankSortReducer.class);

        computeSort_job.setMapOutputKeyClass(DoubleWritable.class);
        computeSort_job.setMapOutputValueClass(Text.class);
        computeSort_job.setOutputKeyClass(Text.class);
        computeSort_job.setOutputValueClass(DoubleWritable.class);

        computeSort_job.setInputFormatClass(TextInputFormat.class);
        computeSort_job.setOutputFormatClass(TextOutputFormat.class);

        //wait
        success = computeSort_job.waitForCompletion(true);
        if (success)
            System.out.println("Job completed successfully: ranking calculated");
        else {
            System.out.println("Job failed: ranking not calculated");
            System.exit(0);
        }

        System.exit(1);
    }
}