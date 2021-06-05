package it.unipi.spark.utils;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class used to parse links and titles from a row
 */
public class CustomPattern {

    private static final Pattern title_pat = Pattern.compile("<title>(.*)</title>");
    private static final Pattern text_pat = Pattern.compile("<text(.*?)</text>");
    private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");
    private static ArrayList<String> outlinks = new ArrayList<>();

    //:::::::::::::::::::::::::::::::::: metodi utilizzati dal job 1 :::::::::::::::::::::::::::::::::::::

    /**
     * Get the title from a row
     * @param str row in xml format
     * @return title of the link
     */
    public static String getTitleContent(String str) {
        Matcher title_match = title_pat.matcher(str);

        //if title exists
        if (title_match.find()) {
            return title_match.group(1).trim();
        } else {
            return null;
        }
    }

    /**
     * Get outlink from a row
     * @param str row in xml format
     * @param title title of the row parsed before
     * @return ArrayList of outlinks
     */
    public static ArrayList<String> getOutlinks(String str, String title) {

        outlinks = new ArrayList<>();

        //retrieve text from str
        Matcher text_match = text_pat.matcher(str);

        if (text_match.find()) {
            String text = text_match.group(1);
            //retrieve all the outlinks
            Matcher outlinks_match = link_pat.matcher(text);
            while (outlinks_match.find()) {

                //get true link
                String outlinkTmp = outlinks_match.group(1);

                /*
                 * There is the possibility that some links contains other links. For example
                 * [[ aLink [[ anotherLink ]] ]]. Parsing these links may cause some errors and the inner links may not
                 * be recognized. So we delete the inner links while we parse the outer links, so we don't have the
                 * inner one already in the list when we parse it.
                 */

                int lastDoubleSquaredBracketsPosition = outlinkTmp.lastIndexOf("[[");

                if (lastDoubleSquaredBracketsPosition != -1) {
                    outlinkTmp = outlinkTmp.substring(0, lastDoubleSquaredBracketsPosition);
                }

                /*
                 * There is also the possibility that a wiki link is a "wiki piped link". For example the user can write a link
                 * using [[TrueLink | myCustomName]] to make it appear as "myCustomName" when the page is saved.
                 * Nevertheless "myCustomName is not a real page, rather "TrueLink" is the real page.
                 * Since different users can use different "myCustomName" then take "TrueLink" as outlink.
                 */

                int lastPipePosition = outlinkTmp.lastIndexOf("|");

                if (lastPipePosition != -1) {
                    outlinkTmp = outlinkTmp.substring(0, lastPipePosition);
                }

                outlinkTmp = outlinkTmp.trim();
                // don't insert the outlink if is already in the list or it's an autoreference
                if (!outlinks.contains(outlinkTmp) && outlinkTmp.compareTo(title) != 0)
                    outlinks.add(outlinkTmp);
            }
        }

        return outlinks;
    }
}