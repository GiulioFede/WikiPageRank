package it.unipi.hadoop.dataModel;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomPattern {

    private static final Pattern title_pat = Pattern.compile("<title>(.*)</title>");
    private static final Pattern text_pat = Pattern.compile("<text(.*?)</text>");
    private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");

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

        String outlinks = "";

        //retrieve text from str
        Matcher text_match = text_pat.matcher(str);
        text_match.matches();
        if(text_match.find()){
            String text = text_match.group(1);
            //retrieve all the outlinks
            Matcher outlinks_match = link_pat.matcher(text);
            while(outlinks_match.find()){
                //here i have one link
                outlinks+="*$*"+outlinks_match.group(1);
            }

            outlinks+="*$*";
        }

        return outlinks;

    }
}