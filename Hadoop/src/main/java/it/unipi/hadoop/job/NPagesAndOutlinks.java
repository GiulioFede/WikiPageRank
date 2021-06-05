package it.unipi.hadoop.job;

import it.unipi.hadoop.dataModel.CustomCounter;
import it.unipi.hadoop.dataModel.Node;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class NPagesAndOutlinks {

    private static final Pattern title_pat = Pattern.compile("<title>(.*)</title>");
    private static final Pattern text_pat = Pattern.compile("<text(.*?)</text>");
    private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");

     /**
     *  Mappers directly save to HDFS (Map-Only job)
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

            //get line of file
            String line = value.toString();

            //get content of title
            String titlePage = getTitleContent(line);

            if(titlePage!=null){
                node.setOutlinks(getOutlinks(line,titlePage));
                //to avoid saving also the default fields of the Node class (thus avoid wasting space on HDFS) we send only the outlinks
                context.write(new Text(titlePage.trim()), node);

                //increment number of pages
                context.getCounter(CustomCounter.NUMBER_OF_PAGES).increment(1);
            }
        }

    }

/**
 * Why we don't use the reducer? We suppose that in the file each page is on a single line and that therefore the same is not repeated in other lines
 * */

    public static String getTitleContent(String str){
        Matcher title_match = title_pat.matcher(str);

        //if title exists
        if(title_match.find())
            return title_match.group(1);
        else
            return null;
    }

    public static String getOutlinks(String str, String title){

        StringBuilder outlinks = new StringBuilder();

        //retrieve text from str
        Matcher text_match = text_pat.matcher(str);

        if(text_match.find()){
            String text = text_match.group(1);
            //retrieve all the outlinks
            Matcher outlinks_match = link_pat.matcher(text);
            while(outlinks_match.find()){
                /**
                 * There is the possibility that a wiki link is a "wiki piped link". For example the user can write a link
                 * using [[TrueLink | myCustomName]] to make it appear as "myCustomName" when the page is saved.
                 * Nevertheless "myCustomName is not a real page, rather "TrueLink" is the real page.
                 * Since different users can use different "myCustomName" then take "TrueLink" as outlink.
                 */
                //get true link
                String outlinkTmp = outlinks_match.group(1);

                int lastDoubleSquaredBracketsPosition = outlinkTmp.lastIndexOf("[[");

                if(lastDoubleSquaredBracketsPosition != -1){
                    outlinkTmp = outlinkTmp.substring(0, lastDoubleSquaredBracketsPosition);
                }

                int lastPipePosition = outlinkTmp.lastIndexOf("|");

                if(lastPipePosition!=-1) {
                    outlinkTmp = outlinkTmp.substring(0, lastPipePosition);
                }

                if(!outlinks.toString().contains("[[" + outlinkTmp + "]]") && outlinkTmp.compareTo(title)!=0) {
                    //here i have one link
                    outlinks.append("[[").append(outlinkTmp).append("]]");
                }
            }
        }

        return outlinks.toString();
    }

}
