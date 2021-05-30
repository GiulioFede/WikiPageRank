package it.unipi.hadoop.test;

import it.unipi.hadoop.dataModel.Node;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    public static void main(String[] args) {
        /*
        String file_xml = "     <title>1984 world oil market chronology</title>     <id>4238068</id>     <revision>       <id>241309816</id>       <timestamp>2008-09-27T12:08:35Z</timestamp>       <contributor>         <username>Jamcib</username>         <id>3052111</id>       </contributor>       <text xml:space=\"preserve\">*'''February - March''': [[Iran]] captures Najnoon Islands. *'''[[March 27]]''': Beginning of &quot;tanker war.&quot; Over the next nine months, 44 ships, including [[Iran|Iranian]], [[Iraqi]], [[Saudi Arabian]] and [[Kuwaiti]] tankers, are attacked by Iraqi or Iranian warplanes or damaged by [[sea mine|mines]]. *'''March - June''':   Iran mobilizes 500,000 troops to southern front. No offensive materializes. *'''[[May 26]]''': President [[Ronald Reagan]] rules out [[U.S.]] military intervention. *'''June''': Civilian target truce in Iran-Iraq war. *'''October''': [[Norway]] and [[United Kingdom|Britain]] cut prices in response to falling spot market. [[Nigeria]] follows, renewing pressure on [[OPEC]] price cuts. *'''[[October 17]]''': OPEC cuts production to 16 MMB/D, but agreement is negated by cheating and price-discounting.  {{start box}} |- | width=&quot;30%&quot; align=&quot;center&quot; | previous year:&lt;br /&gt;[[1983 world oil market chronology]] | width=&quot;40%&quot; align=&quot;center&quot; | '''This article is part of the'''&lt;br /&gt;'''[[Chronology of world oil market events (1970-2005)]]''' | width=&quot;30%&quot; align=&quot;center&quot; | following year:&lt;br /&gt;[[1985 world oil market chronology]] |- {{end box}}  [[Category:History of the petroleum industry]] [[Category:1984 in economics|World oil market chronology]]   {{hist-stub}}</text>     </revision>";
        Pattern title_pat = Pattern.compile("<title>(.*)</title>"); //match tutto ciò che inizia con <title> e finisce con <title>, qualsiasi sia il contenuto interno
        Pattern text_pat = Pattern.compile("<text(.*?)</text>");
        Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");
        Matcher title_match = text_pat.matcher(file_xml);
        title_match.matches();
        if(title_match.find()) {
            System.out.println(title_match.group(1));
            String content = title_match.group(1);
            Matcher link_match = link_pat.matcher(content);
            String outlinks = "";
            while(link_match.find()) {
                System.out.println(link_match.group(1));

                outlinks+=link_match.group(1)+";";
            }
            System.out.println(outlinks);


        }
        else
            System.out.println(title_match.find());

*/
        String value = ("Category:2004 in Hong Kong and Macau    //SEPARATOR//[[Category:Years in Hong Kong and Macau]][[Category:2004 by country|Hong]]//SEPARATOR//-1.0//SEPARATOR//-1.0//SEPARATOR//");

        String SEPARATOR = "//SEPARATOR//";
        Pattern separator_pat = Pattern.compile("(.*?)"+SEPARATOR);
        Pattern outlinks_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");

        Matcher match = separator_pat.matcher(value.toString());
        if(match.find())
            System.out.println(match.group(1));
        if(match.find())
            System.out.println(match.group(1));
        if(match.find())
            System.out.println(Double.parseDouble(match.group(1)));
        if(match.find())
            System.out.println(Double.parseDouble(match.group(1)));


        /*

        System.out.println(outlinks.length);
        System.out.println(row[0]);
        System.out.println(row[1]);
        System.out.println(outlinks[0]);
        System.out.println(outlinks[1]);
        System.out.println(outlinks[2]);
        */
    }


}
