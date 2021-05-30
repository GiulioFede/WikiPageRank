package it.unipi.hadoop.dataModel;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomPattern {

    private static final Pattern title_pat = Pattern.compile("<title>(.*)</title>");
    private static final Pattern text_pat = Pattern.compile("<text(.*?)</text>");
    private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");
    public static final String SEPARATOR = "//SEPARATOR//";
    private static final Pattern separator_pat = Pattern.compile("(.*?)//SEPARATOR//");

    //:::::::::::::::::::::::::::::::::: metodi utilizzati dal job 1 :::::::::::::::::::::::::::::::::::::

    public static String getTitleContent(String str){
        Matcher title_match = title_pat.matcher(str);
        title_match.matches();

        //if title exists
        if(title_match.find())
            return title_match.group(1);
        else
            return null;
    }

    public static String getOutlinks(String str){

        //String outlinks = SEPARATOR;
        String outlinks = "";

        //retrieve text from str
        Matcher text_match = text_pat.matcher(str);
        text_match.matches();
        if(text_match.find()){
            String text = text_match.group(1);
            //retrieve all the outlinks
            Matcher outlinks_match = link_pat.matcher(text);
            while(outlinks_match.find()){
                if(!outlinks.contains(outlinks_match.group(1)))
                    //here i have one link
                    outlinks += "[["+outlinks_match.group(1)+"]]";
            }
        }

        //outlinks += SEPARATOR;

        return outlinks;
    }

    //:::::::::::::::::::::::::::::::::::::: metodi utilizzati dal job 2 :::::::::::::::::::::::::::::::

    public static void getTitleOutlinksRankRankReceive(String title,
                                                       String outlinks
                                                       ){

    }

    public static String getTargettContent(String str, String target){

        Matcher match = separator_pat.matcher(str);
        if(match.find()){
            if(target=="title") return match.group(1);
            if(match.find()){
                if(target=="outlinks") return match.group(1);
                if(match.find()){

                }
            }
        }

        return null;
    }

}