package it.unipi.hadoop.job;

import it.unipi.hadoop.dataModel.CustomCounter;
import it.unipi.hadoop.dataModel.CustomPattern;
import it.unipi.hadoop.dataModel.Node;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class NPagesAndOutlinks {

    /*
        Sono i mapper che direttamente salvano su HDFS
     */


    //::::::::::::::::::::::::::::::::::::::: MAPPER :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    public static class NPagesAndOutlinksMapper extends Mapper<LongWritable, Text, Text, Node>
    {
        Node node;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            node = new Node();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);

            //get line of file
            String line = value.toString();

            //get content of title
            String titlePage = CustomPattern.getTitleContent(line);

            if(titlePage!=null){
                node.setOutlinks(CustomPattern.getOutlinks(line));
                context.write(new Text(titlePage), node);

            }

            //increment number of pages
            context.getCounter(CustomCounter.NUMBER_OF_PAGES).increment(1);
        }
    }

    /*
            Perchè non utilizziamo il reducer: supponiamo che nel file ogni pagina stia su una sola riga e che quindi non si ripeta la stessa
                                               pagina su più righe
            TODO: chiedere al professore se dobbiamo collezionare outlink
     */
}
