package utils;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomPattern {

    private static final Pattern title_pat = Pattern.compile("<title>(.*)</title>");
    private static final Pattern text_pat = Pattern.compile("<text(.*?)</text>");
    private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");
    public static final String SEPARATOR = "//SEPARATOR//";
    private static final Pattern separator_pat = Pattern.compile("(.*?)//SEPARATOR//");
    private static ArrayList<String> outlinks = new ArrayList<>();

    //:::::::::::::::::::::::::::::::::: metodi utilizzati dal job 1 :::::::::::::::::::::::::::::::::::::

    public static String getTitleContent(String str) {
        Matcher title_match = title_pat.matcher(str);

        //if title exists
        if (title_match.find())
            return title_match.group(1).trim();
        else
            return null;
    }

    public static ArrayList<String> getOutlinks(String str, String title) {

        outlinks.clear();

        //retrieve text from str
        Matcher text_match = text_pat.matcher(str);

        if (text_match.find()) {
            String text = text_match.group(1);
            //retrieve all the outlinks
            Matcher outlinks_match = link_pat.matcher(text);
            while (outlinks_match.find()) {
                /**
                 * There is the possibility that a wiki link is a "wiki piped link". For example the user can write a link
                 * using [[TrueLink | myCustomName]] to make it appear as "myCustomName" when the page is saved.
                 * Nevertheless "myCustomName is not a real page, rather "TrueLink" is the real page.
                 * Since different users can use different "myCustomName" then take "TrueLink" as outlink.
                 */
                //get true link
                String outlinkTmp = outlinks_match.group(1);

                int lastDoubleSquaredBracketsPosition = outlinkTmp.lastIndexOf("[[");

                if (lastDoubleSquaredBracketsPosition != -1) {
                    outlinkTmp = outlinkTmp.substring(0, lastDoubleSquaredBracketsPosition);
                }

                int lastPipePosition = outlinkTmp.lastIndexOf("|");

                if (lastPipePosition != -1) {
                    outlinkTmp = outlinkTmp.substring(0, lastPipePosition);
                }

                outlinkTmp = outlinkTmp.trim();
                if (!outlinks.contains(outlinkTmp) && outlinkTmp.compareTo(title) != 0)
                    outlinks.add(outlinkTmp);
            }
        }

        return outlinks;
    }
}