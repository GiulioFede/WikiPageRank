import dataModel.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.CustomPattern;

import java.util.ArrayList;

public class WikiPageRank {

    private static ArrayList<String> outlinks = new ArrayList<>();
    private static String title;
    private static Pair row = new Pair();

    public static void main(String[] args) {

        SparkConf configuration = new SparkConf().setAppName("WikiPageRank").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(configuration);
        sc.setLogLevel("ERROR");

        System.out.println("::::::::::::::: Wiki Page Rank :::::::::::::::");

        //load wiki
        JavaRDD<String> wiki_rdd = sc.textFile("hdfs://namenode:9820/user/hadoop/input/wiki-micro.txt");

        JavaRDD<Pair> titles_rdd = wiki_rdd.map(page -> parsePages(page)).cache();
        System.out.println(titles_rdd.collect());
        sc.stop();
    }

    private static Pair parsePages(String page) {

        //clear
        outlinks.clear();

        //get title
        title = CustomPattern.getTitleContent(page);
        //get text
        outlinks = CustomPattern.getOutlinks(page,title);

        //initialize row object
        row.setTitle(title);
        row.setOutlinks(outlinks);

        return row;

    }
}