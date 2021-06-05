package it.unipi.hadoop.job;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RankSort {

    public static class RankSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
    {

        private static final String SEPARATOR = "//SEPARATOR//";
        private static final Pattern separator_pat = Pattern.compile("(.*?)"+SEPARATOR);
        private static final Pattern outlinks_pat2 = Pattern.compile(SEPARATOR+"(.*?)"+SEPARATOR);

        private String title;
        private double rank;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            title = "";
            rank = 0;
        }

        /**
         * Tipo di linea ricevuta:
         * titolo ||SEPARATOR||[[link1]][[link2]]...[[linkN]]||SEPARATOR||rank||SEPARATOR||rankReceived||SEPARATOR||
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //create Node from input
            Matcher match = separator_pat.matcher(value.toString());
            if(match.find()) {
                title = match.group(1);
                match.find(); // FIX: serve?
            }
            if(match.find())
                rank = Double.parseDouble(match.group(1));

            context.write(new DoubleWritable(-rank),new Text(title));

        }
    }

    public static class RankSortReducer extends Reducer<DoubleWritable,Text, Text, DoubleWritable>
    {

        ArrayList<String> titles;
        int i;


        /**
         * Input:
         * key, [title1, title2,...,titleN]
         */
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            titles = new ArrayList<>();
            for(Text title : values){
               titles.add(title.toString());
            }

            for(i=0; i<titles.size(); i++)
                context.write(new Text(titles.get(i)),new DoubleWritable(-1*key.get()));

        }
    }
}

