package it.unipi.hadoop.job;

import it.unipi.hadoop.dataModel.Node;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRank {

    private static final String SEPARATOR = "//SEPARATOR//";
    private static final Pattern separator_pat = Pattern.compile("(.*?)"+SEPARATOR);
    private static final Pattern internal_outlinks_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");
    private static final Pattern outlinks_pat2 = Pattern.compile(SEPARATOR+"(.*?)"+SEPARATOR);

    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Node>
    {
        long numberOfPages;
        String title;
        String outlinks;
        double rank;
        Node node;
        int i;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            numberOfPages = Integer.parseInt(context.getConfiguration().get("number_of_pages"));
            node = new Node();

            i = 0;
        }

        /**
         * Type of line received:
         * title ||SEPARATOR||[[link1]][[link2]]...[[linkN]]||SEPARATOR||rank||SEPARATOR||rankReceived||SEPARATOR||
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //create Node from input
            Matcher match = separator_pat.matcher(value.toString());
            Matcher outlinks_match = outlinks_pat2.matcher(value.toString());
            if(match.find()) {
                title = match.group(1);
                match.find();
            }
            if(outlinks_match.find())
                outlinks = outlinks_match.group(1);
            if(match.find())
                rank = Double.parseDouble(match.group(1));

            //if it's the first iteration initialize the rank with 1/N
            if(rank==-1)
                rank = (1/((double)(numberOfPages)));

            node.setPageRank(rank);
            node.setPageRankReceived(-1);
            node.setOutlinks(outlinks);
            //emit key node with its information
            context.write(new Text(title.trim()),node);

            node.setOutlinks("");

            //if there are outlinks --> [[link1]][[link2]]...[[linkN]]
            if(outlinks.compareTo("")!=0){
                Matcher internal_outlinks = internal_outlinks_pat.matcher(outlinks);
                Matcher internal_outlinks_count = internal_outlinks_pat.matcher(outlinks);

                //count number of outlinks
                i = 0;
                while (internal_outlinks_count.find()) i++;
                while (internal_outlinks.find()){
                    node.setPageRankReceived((rank/ i));
                    context.write(new Text(internal_outlinks.group(1).trim()),node);
                }
            }

        }
    }

    public static class PageRankReducer extends Reducer<Text,Node,Text,Node>
    {
        Node node;
        double sum;
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

            sum = 0;
            node = new Node();

            child_list = new ArrayList<>();
            for(Node child : values)
                child_list.add(Node.copy(child));

            for(i=0; i<child_list.size(); i++){
                //if the node structure reports information about the outlinks of the key node
                if(child_list.get(i).getPageRankReceived()==-1)
                    node = child_list.get(i);
                else
                    //sum all the contributions received from other nodes that are linked with the key node ( otherNode->keyNode)
                    sum+=child_list.get(i).getPageRankReceived();
            }

            //compute new page rank
            newPageRank = (dampingFactor * (1 / ((double) (numberOfPages)))) + ((1-dampingFactor) * sum);

            if((100*((node.getPageRank()-newPageRank)/node.getPageRank())) > 1 && !context.getConfiguration().getBoolean("Convergence", false))
                context.getConfiguration().setBoolean("Convergence", true);

            node.setPageRank(newPageRank);
            context.write(key,node);

        }
    }
}
















