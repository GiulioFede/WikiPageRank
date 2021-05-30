package it.unipi.hadoop.job;

import it.unipi.hadoop.dataModel.CustomCounter;
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

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.getCounter(CustomCounter.ULTIMO_MAP).increment(1);
            /*
                    Tipo di linea ricevuta: titolo ||SEPARATOR||[[link1]][[link2]]...[[linkN]]||SEPARATOR||rank||SEPARATOR||rankReceived||SEPARATOR||
            */

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



            //se è la prima iterazione settiamo il rank come 1/N con N numero di pagine
            if(rank==-1)
                rank = (1/((double)(numberOfPages)));

            node.setPageRank(rank);
            node.setPageRankReceived(-1);
            node.setOutlinks(outlinks);
            //emetto nodo chiave con le sue informazioni
            context.write(new Text(title),node);

            //riutilizzo node per i figli
            node.setOutlinks("");

            //se esistono outlinks...
            if(outlinks.compareTo("")!=0){
                //qui li avremo in questa forma --> [[link1]][[link2]]...[[linkN]]
                Matcher internal_outlinks = internal_outlinks_pat.matcher(outlinks);
                Matcher internal_outlinks_count = internal_outlinks_pat.matcher(outlinks);

                //count number of outlinks
                i = 0;
                while (internal_outlinks_count.find()) i++;
                while (internal_outlinks.find()){
                    node.setPageRankReceived((rank/ i));
                    context.write(new Text(internal_outlinks.group(1)),node);
                }
            }

/*
            Matcher outlinks_match = outlinks_pat.matcher(outlinks);
            Matcher outlinks_count = outlinks_pat.matcher(outlinks);

            i = 0;
            while(outlinks_count.find())
                i++;

            while(outlinks_match.find()){
                node.setPageRankReceived((rank/ i));
                context.write(new Text(outlinks_match.group(1)),node);
            }

 */

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
            context.getCounter(CustomCounter.UTLIMO_REDUCE).increment(1);

            sum = 0;

            child_list = new ArrayList<Node>();
            for(Node child : values)
                child_list.add(Node.copy(child));

            if(child_list.size()>1)
                context.getCounter(CustomCounter.NUMBER_OF_CHILD).increment(child_list.size());
/*
            context.write(key,new Node());
            //da eliminare
            for(i=0; i<child_list.size();i++){

                context.write(key,child_list.get(i));
            }*/



            for(i=0; i<child_list.size(); i++){
                //se è un nodo che riporta le informazioni
                if(child_list.get(i).getPageRankReceived()==-1)
                    node = Node.copy(child_list.get(i));
                else
                    sum+=child_list.get(i).getPageRankReceived();

                context.getCounter(CustomCounter.SUM).increment((long)child_list.get(i).getPageRankReceived());
            }

            //context.getCounter(CustomCounter.SUM).increment(sum);
            //calcolo nuovo page rank
            newPageRank = dampingFactor*(1/((double)(numberOfPages))) + (1-dampingFactor)*sum;
            node.setPageRank(newPageRank);

            context.write(key,node);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}
















