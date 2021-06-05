package it.unipi.hadoop.job;

import it.unipi.hadoop.dataModel.CustomCounter;
import it.unipi.hadoop.dataModel.CustomPattern;
import it.unipi.hadoop.dataModel.Node;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class NPagesAndOutlinks {

     /**
     *  Sono i mapper che direttamente salvano su HDFS (Map-Only job)
     */

    //::::::::::::::::::::::::::::::::::::::: MAPPER :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    public static class NPagesAndOutlinksMapper extends Mapper<LongWritable, Text, Text, Node>
    {
        Node node;
        ArrayList<String> children;
        int i;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            node = new Node();
            i = 0;
            children = new ArrayList<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //get line of file
            String line = value.toString();
            node = new Node();

            //get content of title
            String titlePage = CustomPattern.getTitleContent(line);

            if(titlePage!=null){
                node.setOutlinks(CustomPattern.getOutlinks(line,titlePage));
                //send father
                context.write(new Text(titlePage.trim()), node);

                //send each child
                children = new ArrayList<>();
                children = CustomPattern.getListOfOutlinks(line,titlePage);
                node = new Node();
                node.setPageRankReceived(-2);
                for(i = 0; i<children.size(); i++){
                    context.write(new Text(children.get(i)),node);
                }
            }
        }

    }

    public static class NPagesAndOutlinksReducer extends Reducer<Text,Node,Text,Node>
    {

        Node node;
        ArrayList<Node> nodes;
        int i;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            node = new Node();
            nodes = new ArrayList<>();
            i=0;
        }

        @Override
        protected void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {

            //increment number of nodes
            context.getCounter(CustomCounter.NUMBER_OF_PAGES).increment(1);

            node = new Node();
            nodes = new ArrayList<>();

            for(Node node : values){
                nodes.add(Node.copy(node));
            }

            boolean trovato = false;
            for(i=0; i<nodes.size();i++){
                //se il nodo ricevuto indica la struttura del key node
                if(nodes.get(i).getPageRankReceived()!=-2) {
                    node = nodes.get(i);
                    trovato = true;
                }
            }

            if(trovato)
                context.write(key,node);

        }
    }

}
