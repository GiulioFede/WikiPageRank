import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import utils.CustomPattern;

import java.io.Serializable;
import java.util.*;


import java.util.ArrayList;

public class WikiPageRank implements Serializable {

    private static ArrayList<String> outlinks = new ArrayList<>();
    private static String title;

    public static void main(String[] args) {

        SparkConf configuration = new SparkConf().setAppName("WikiPageRank").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(configuration);
        sc.setLogLevel("ERROR");

        System.out.println("::::::::::::::: Wiki Page Rank :::::::::::::::");

        //load wiki
        JavaRDD<String> wiki_rdd = sc.textFile("hdfs://namenode:9820/user/hadoop/input/wiki-micro.txt");
        System.out.println("file letto");
        System.out.println(wiki_rdd.take(20));
        //initialize rdd with pairs of title and outlinks
        JavaPairRDD<String,ArrayList<String>> titles_rdd = wiki_rdd.mapToPair(new PairFunction<String, String, ArrayList<String>>() {
            @Override
            public Tuple2<String, ArrayList<String>> call(String page) throws Exception {

                //clear
                outlinks.clear();

                //get title
                title = CustomPattern.getTitleContent(page);
                //get text
                outlinks = CustomPattern.getOutlinks(page,title);

                return new Tuple2<String, ArrayList<String>>(title,outlinks);
            }
        }).cache();
        System.out.println("titoli e outlinks prelevati");
        //count number of page
        long numberOfPages = titles_rdd.count();
        System.out.println("numero di pagine "+numberOfPages);
        //initialize each title with first rank (1/n)
        JavaPairRDD<String,Double> ranks = titles_rdd.mapValues(new Function<ArrayList<String>, Double>() {
            @Override
            public Double call(ArrayList<String> strings) throws Exception {

                return (1.0/numberOfPages);
            }
        });
        System.out.println("primo rank fatto");
        System.out.println(ranks.take(20));
        //loop
        for(int i =0; i<10; i++){
            System.out.println("Ciclo "+i);
            JavaPairRDD<String,Double> contributions = titles_rdd.join(ranks).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<ArrayList<String>, Double>>, String, Double>() {
                @Override
                public Iterator<Tuple2<String, Double>> call(Tuple2<String, Tuple2<ArrayList<String>, Double>> tuple) throws Exception {
                    ArrayList<String> tuple_outlinks = tuple._2._1;
                    int num_outlinks = tuple_outlinks.size();
                    ArrayList<Tuple2<String,Double>> list = new ArrayList<>();
                    list.add(new Tuple2<String, Double>(tuple._1,0.0));
                    for(int j=0; j<num_outlinks;j++)
                        list.add(new Tuple2(tuple_outlinks.get(j),tuple._2._2/num_outlinks));

                    return list.iterator();
                }
            });
            System.out.println("contributions");
            System.out.println(contributions.take(20));

            //update ranks
            ranks = contributions.reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double x, Double y) throws Exception {
                    return (x + y);
                }
            }).mapValues(new Function<Double, Double>() {
                @Override
                public Double call(Double lastRank) throws Exception {
                    return computeNewRank(lastRank,numberOfPages);
                }
            });
            System.out.println("ranks");
            System.out.println(ranks.take(20));
        }
        System.out.println("ciclo terminato");
        //sorting
        List<Tuple2<String,Double>> pageRanksOrdered = ranks.takeOrdered(((int)(ranks.count())), MyTupleComparator.INSTANCE);


        System.out.println("sorting fatto");
        //save
        JavaRDD<Tuple2<String,Double>> pageRanksOrderedRdd = sc.parallelize(pageRanksOrdered);
        System.out.println("parallelizzazione fatta");
        pageRanksOrderedRdd.saveAsTextFile("sparkJavaOutput.txt");
        System.out.println("file salvato");

        sc.stop();
    }

    private static Double computeNewRank(Double lastRank, long numberOfPages){
        return (0.15*(1/numberOfPages) + 0.85*lastRank);
    }

    static class MyTupleComparator implements
            Comparator<Tuple2<String, Double>>, Serializable {
        final static MyTupleComparator INSTANCE = new MyTupleComparator();
        // note that the comparison is performed on the key's frequency
        // assuming that the second field of Tuple2 is a count or frequency
        public int compare(Tuple2<String,Double> t1,
                           Tuple2<String, Double> t2) {
            return -t1._2.compareTo(t2._2);    // sort descending
            // return t1._2.compareTo(t2._2);  // sort ascending
        }
    }

}