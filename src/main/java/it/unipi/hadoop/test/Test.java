package it.unipi.hadoop.test;

import it.unipi.hadoop.dataModel.CustomCounter;
import it.unipi.hadoop.dataModel.CustomPattern;
import it.unipi.hadoop.dataModel.Node;
import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static it.unipi.hadoop.dataModel.CustomPattern.SEPARATOR;

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
        String value2 = ("Category:2004 in Hong Kong and Macau    //SEPARATOR////SEPARATOR//-1.0//SEPARATOR//-1.0//SEPARATOR//");
        String wiki = ("     <title>Edward Roschmann</title>     <id>4215075</id>     <revision>       <id>240637538</id>       <timestamp>2008-09-24T08:12:06Z</timestamp>       <contributor>         <username>AlleborgoBot</username>         <id>3813685</id>       </contributor>       <minor />       <comment>robot  Adding: [[it:Edward Roschmann]]</comment>       <text xml:space=\"preserve\">[[Image:Roschmann eduard 147.jpg|thumb|right|200px|Edward Roschmann]]  '''Edward Roschmann''' (also ''Eduard Roschmann'') ([[25 August]] [[1908]] - [[10 August]] [[1977]]) was an [[Schutzstaffel|SS]] commander, holding the rank of ''[[Hauptsturmführer]]'' and one of the heads of the [[concentration camp]] at [[Riga]], used to persecute [[German Jews]]. Infamous as &quot;The Butcher of Riga,&quot;&lt;ref name=&quot;BR&quot;&gt;[http://www.thirdworldtraveler.com/Latin_America/ClockworkOrange_COTP.html Roschmann hiding in South America]&lt;/ref&gt; Roschmann was sought for [[war crimes]] and remained a fugitive for the rest of his life. Hiding for the most part in [[Argentina]], Roschmann is speculated to have been a leader of the post-war [[ODESSA]] network of fugitive SS officers.   ==Early life== Roschmann was born on [[August 25]], [[1908]] in [[Graz]], [[Austria]]. Failing in his attempt to become a lawyer, Roschmann worked at a brewery and became active in the [[Austrian Nazis|Austrian Nazi Party]].&lt;ref&gt;{{cite book  | last = Forsyth  | first = Frederick  | title = The Odessa File  | pages = 126  | year = 1972  | publisher = Viking Press }}&lt;/ref&gt; Following the [[Anschluss|annexation of Austria]] by Germany in 1938, Roschmann was inducted into the [[Schutzstaffel|Schutzstaffel (SS)]] and later promoted to the rank of [[hauptsturmführer]] #152681.  ==Riga concentration camp== In 1941, Roschmann was placed in second-in-command of the concentration camp&lt;ref name=&quot;CC&quot;&gt;[http://query.nytimes.com/gst/fullpage.html?res=9C0CE1DF133FF930A25756C0A966958260&amp;sec=&amp;spon=&amp;pagewanted=2 Eduard Roschmann in the SS]&lt;/ref&gt; in Riga, [[Latvia]], which held German Jews during [[World War II]]. It is reported that Roschmann had many women, children and elderly exterminated on their arrival to the camp, claiming them to be more valuable dead. The clothes, hair and fillings of the dead were treated as a cash asset, to be utilized for production of various materials. During his tenure, Roschmann supervised the killing of an estimated 35,000 people.&lt;ref name=&quot;CC&quot;&gt;[http://query.nytimes.com/gst/fullpage.html?res=9C0CE1DF133FF930A25756C0A966958260&amp;sec=&amp;spon=&amp;pagewanted=2 Eduard Roschmann in the SS]&lt;/ref&gt; The Riga Camp inmates labored eighteen hours a day at the camp workshops, with their suffering exacerbated by hunger and cold. Roschmann would strip prisoners of their clothes and huddle them together before [[execution]]. He also liked watching dogs feed on them while they were still breathing. The exposure of his activities earned him the infamous title of Butcher of Riga.  ==Escape and death== The advance of the [[USSR]]'s [[Red Army]] prompted Roschmann to move westwards with his SS unit, taking along the living inmates of the camp. The grueling retreat claimed the lives of thousands of the remaining inmates, who had virtually no food or clothing and being marched through snow-laden routes in freezing cold. With the approaching defeat of Germany, Roschmann escaped from the Soviet and Allied authorities in the guise of an army corporal. Sheltering amongst friends in Austria, he was twice apprehended by Allied authorities, but managed to escape both times.&lt;ref&gt;{{cite book  | last = Forsyth  | first = Frederick  | title = The Odessa File  | pages = 152-56  | year = 1972  | publisher = Viking Press }}&lt;/ref&gt; Making contact with an organization of SS fugitives, he was smuggled to a monastery in [[Rome]], which served as a major hiding place under the auspices of the monk [[Alois Hudal]].&lt;ref&gt;{{cite book  | last = Forsyth  | first = Frederick  | title = The Odessa File  | pages = 172  | year = 1972  | publisher = Viking Press }}&lt;/ref&gt;   Roschmann then travelled to [[Juan Peron]]'s Argentina, where the sympathetic government gave him citizenship. Roschmann was forced to leave Argentina in 1977 after [[West Germany]] requested his extradition. Shifting to [[Paraguay]], Roschmann died of a heart attack later in the year.  ==ODESSA== Roschmann is believed to have remained active in the [[ODESSA]] network, which aided SS refugees and attempted to re-infiltrate the institutions of modern Germany. He was featured as the main antagonist in [[Frederick Forsyth]]'s novel, ''[[The Odessa File]]'', and later in the [[The Odessa File (film)|1974 film]] based on the novel. His character was played by the [[Academy Award]]-winning Austrian actor [[Maximilian Schell]].&lt;ref name=&quot;SW&quot;&gt;[http://query.nytimes.com/gst/fullpage.html?res=9C0CE1DF133FF930A25756C0A966958260&amp;sec=&amp;spon=&amp;pagewanted=2 Wiesenthal suggests use of Roschmann in novel]&lt;/ref&gt; The novel describes much of Roschmann's actual life and deeds; the use of the character was suggested by the Nazi-hunter [[Simon Wiesenthal]], who believed that Forsyth's book could help track down the fugitive.&lt;ref name=&quot;SW&quot;&gt;[http://query.nytimes.com/gst/fullpage.html?res=9C0CE1DF133FF930A25756C0A966958260&amp;sec=&amp;spon=&amp;pagewanted=2 Wiesenthal suggests use of Roschmann in novel]&lt;/ref&gt;  ==References== {{reflist}}  ==External links== *[http://forum.axishistory.com/viewtopic.php?t=3454 Roschmann Photo and biography] *[http://query.nytimes.com/gst/fullpage.html?res=9C0CE1DF133FF930A25756C0A966958260&amp;sec=&amp;pagewanted=1 New York Times article with information about Roschmann's post-war movements] *[http://www.thirdworldtraveler.com/Latin_America/ClockworkOrange_COTP.html Post War Movement]  {{DEFAULTSORT:Roschmann, Eduard}} [[Category:1908 births]] [[Category:1977 deaths]] [[Category:Germans of Austrian descent]] [[Category:German military personnel of World War II]] [[Category:SS officers]] [[Category:Holocaust perpetrators]]  [[de:Eduard Roschmann]] [[fr:Eduard Roschmann]] [[it:Edward Roschmann]] [[ja:エドゥアルト・ロシュマン]] [[fi:Edward Roschmann]]</text>     </revision>");
        String wiki2 = ("     <title>Edward Roschmann</title>     <id>4215075</id>     <revision>       <id>240637538</id>       <timestamp>2008-09-24T08:12:06Z</timestamp>       <contributor>         <username>AlleborgoBot</username>         <id>3813685</id>       </contributor>       <minor />       <comment>robot  Adding: [[it:Edward Roschmann]]</comment>       <text xml:space=\"preserve\">fbbjb dfjefewj wfnejfnw</text>     </revision>");
/*
        System.out.println(CustomPattern.getOutlinks(wiki));

        //get line of file
        String line = wiki;
        Node node = new Node();
        //get content of title
        String titlePage = CustomPattern.getTitleContent(line);
        System.out.println("TITOLO: "+titlePage);
        if(titlePage!=null){
            node.setOutlinks(CustomPattern.getOutlinks(line));
            System.out.println("OUTLINKS: "+CustomPattern.getOutlinks(line));
            //to avoid saving also the default fields of the Node class (thus avoid wasting space on HDFS) we send only the outlinks
            System.out.println(node.toString());

*/
        String s = " ciao sono x ";
        System.out.println(s.length());
        System.out.println(s.trim().length());


/*
        String SEPARATOR = "//SEPARATOR//";
        Pattern separator_pat = Pattern.compile("(.*?)"+SEPARATOR);
        Pattern outlinks_pat = Pattern.compile(SEPARATOR+"(.*?)"+SEPARATOR);
        Pattern internal_outlinks_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");


        Matcher match = separator_pat.matcher(value);
        Matcher match_OUTLINKS = outlinks_pat.matcher(value);
        System.out.println(match);
        if(match.find())
            System.out.println(match.group(1));
        if(match_OUTLINKS.find()) {
            //[] [] []
            System.out.println(match_OUTLINKS.group(1));
            String outlink = match_OUTLINKS.group(1);
            Matcher internal_outlinks = internal_outlinks_pat.matcher(outlink);
            while(internal_outlinks.find())
                System.out.println(internal_outlinks.group(1));
        }
        match.find();
        if(match.find())
            System.out.println(Double.parseDouble(match.group(1)));
        if(match.find())
            System.out.println(Double.parseDouble(match.group(1)));
*/

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
