package it.unipi.hadoop.job;

import it.unipi.hadoop.dataModel.CustomCounter;
import it.unipi.hadoop.dataModel.CustomPattern;
import it.unipi.hadoop.dataModel.Node;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class NPagesAndOutlinks {

    /*
        Sono i mapper che direttamente salvano su HDFS
     */


    //::::::::::::::::::::::::::::::::::::::: MAPPER :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    public static class NPagesAndOutlinksMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        Node node;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            node = new Node();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //get line of file
            String line = value.toString();

            //get content of title
            String titlePage = CustomPattern.getTitleContent(line);

            if(titlePage!=null){
                node.setOutlinks(CustomPattern.getOutlinks(line));
                //to avoid saving also the default fields of the Node class (thus avoid wasting space on HDFS) we send only the outlinks
                context.write(new Text(titlePage), new Text(node.getOutlinks()));

                //increment number of pages
                context.getCounter(CustomCounter.NUMBER_OF_PAGES).increment(1);
            }
        }

    }

    public static class NPagesAndOutlinksReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            for(IntWritable val : values)
                context.write(new Text(key), val);
        }
    }

    /*
            Perchè non utilizziamo il reducer: supponiamo che nel file ogni pagina stia su una sola riga e che quindi non si ripeta la stessa
                                               pagina su più righe
            TODO: chiedere al professore se dobbiamo collezionare outlink
     */


}
