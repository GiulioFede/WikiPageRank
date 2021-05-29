package it.unipi.hadoop.job;

import it.unipi.hadoop.dataModel.CustomCounter;
import it.unipi.hadoop.dataModel.Node;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class PageRank {

    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Node>
    {
        long numberOfPages;
        String outlinks;
        double rank;
        double rankReceived;
        Node node;
        int i;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            numberOfPages = Integer.parseInt(context.getConfiguration().get("number_of_pages"));
            node = new Node();
            i = 0;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] row = value.toString().split("###");
            outlinks = row[0];
            rank = Double.parseDouble(row[1]);
            rankReceived = Double.parseDouble(row[2]);

            //se è la prima iterazione settiamo il rank come 1/N con N numero di pagine
            if(rank==-1)
                rank = (1/((double)numberOfPages));

            node.setPageRank(rank);

            node.setOutlinks(outlinks);
            //emetto nodo chiave con le sue informazioni
            String[] outlinks_list = outlinks.split("//:://");
            //context.write(new Text(outlinks_list[0].toString()),node);

            context.write(new Text(String.valueOf(Math.random())),node);
            //riutilizzo node per i figli
            node.setOutlinks("");


            //se possiede outlinks
            if(outlinks_list.length>0){

                for(i=1; i<outlinks_list.length;i++){
                    node.setPageRankReceived(rank/ outlinks_list.length);
                    context.write(new Text(outlinks_list[i]),node);
                }
            }

        }
    }

    public static class PageRankReducer extends Reducer<Text,Node,Text,Node>
    {

        Node node;
        int sum;
        ArrayList<Node> child_list;
        int i;
        static double dampingFactor;
        static int numberOfPages;
        double newPageRank;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            node = new Node();
            sum = 0;
            i = 0;
            dampingFactor = Double.parseDouble(context.getConfiguration().get("damping_factor"));
            newPageRank = 0;
            numberOfPages = Integer.parseInt(context.getConfiguration().get("number_of_pages"));
        }

        @Override
        protected void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {

            child_list = new ArrayList<Node>();
            for(Node child : values)
                child_list.add(Node.copy(child));

            for(i=0; i<child_list.size(); i++){
                //se è un nodo che riporta le informazioni
                if(child_list.get(i).getPageRankReceived()==-1)
                    node = child_list.get(i);
                else
                    sum+=child_list.get(i).getPageRankReceived();
            }

            //calcolo nuovo page rank
            newPageRank = dampingFactor*(1/numberOfPages) + (1-dampingFactor)*sum;
            node.setPageRank(newPageRank);

            context.write(key,node);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}
















