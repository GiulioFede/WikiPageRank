package it.unipi.spark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import it.unipi.spark.utils.CustomPattern;

import java.io.Serializable;
import java.util.*;


import java.util.ArrayList;

/**
 * Java implementation of Page Rank in Spark
 */
public class WikiPageRank implements Serializable {

    private static ArrayList<String> outlinks = new ArrayList<>();
    private static String title;

    /**
     * Compute the new rank to send
     * @param lastRank the previous rank
     * @param numberOfPages N, the total number of nodes
     * @return the new rank calculated
     */
    private static Double computeNewRank(Double lastRank, long numberOfPages){
        return ((0.15*(1.0/((double)(numberOfPages))) + 0.85*lastRank));
    }

    /**
     * Custom tuple comparator class, sort in descending order
     */
    private static class TupleComparator implements
            Comparator<Tuple2<String, Double>>, Serializable {
        final static TupleComparator INSTANCE = new TupleComparator();

        public int compare(Tuple2<String,Double> t1, Tuple2<String, Double> t2) {
            return -t1._2.compareTo(t2._2);
        }
    }

    public static void main(String[] args) {
        System.out.println("::::::::::::::: Wiki Page Rank :::::::::::::::");

        SparkConf configuration = new SparkConf().setAppName("WikiPageRank").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(configuration);
        sc.setLogLevel("ERROR");

        if(args.length < 2) {
            System.out.println("Usage: <#iterations> <file_input>");
            System.exit(-1);
        }
        int iterations = Integer.parseInt(args[0]);
        String file_input = args[1];

        JavaRDD<String> wiki_rdd = sc.textFile(file_input);

        // initialize rdd with pairs of title and outlinks
        JavaPairRDD<String,ArrayList<String>> titles_rdd = wiki_rdd.mapToPair
                (
                        // get titles and outlinks
                        (PairFunction<String, String, ArrayList<String>>) page -> {
                            title = CustomPattern.getTitleContent(page);
                            outlinks = CustomPattern.getOutlinks(page,title);
                            return new Tuple2<>(title, outlinks);
                        }
                ).cache(); // save rdd in cache

        long numberOfPages = titles_rdd.count();

        // initialize each title with initial rank (1/n)
        JavaPairRDD<String,Double> ranks = titles_rdd.mapValues
                (
                        (Function<ArrayList<String>, Double>) strings -> (1.0/((double)numberOfPages))
                );

        for(int i = 0; i < iterations; i++){
            /*
             * compute a join between the titles and the ranks, to update the initial rank with the new one
             */
            JavaPairRDD<String,Double> contributions = titles_rdd.join(ranks).flatMapToPair
                    (
                            (PairFlatMapFunction<Tuple2<String, Tuple2<ArrayList<String>, Double>>, String, Double>) tuple -> {
                                ArrayList<String> tuple_outlinks = tuple._2._1;
                                int num_outlinks = tuple_outlinks.size(); // number of outlinks
                                ArrayList<Tuple2<String,Double>> list = new ArrayList<>();
                                list.add(new Tuple2<>(tuple._1, 0.0));
                                for (String tuple_outlink : tuple_outlinks) {
                                    list.add(new Tuple2(tuple_outlink, tuple._2._2 / ((double) (num_outlinks))));
                                }
                                return list.iterator(); // rank to send
                            }
                    );

            //update ranks
            ranks = contributions.reduceByKey((Function2<Double, Double, Double>) Double::sum).mapValues
                    (
                            (Function<Double, Double>) lastRank -> computeNewRank(lastRank,numberOfPages)
                    );
        }

        List<Tuple2<String,Double>> pageRanksOrdered = ranks.takeOrdered(
                ((int)(ranks.count())), TupleComparator.INSTANCE);

        JavaRDD<Tuple2<String,Double>> pageRanksOrderedRdd = sc.parallelize(pageRanksOrdered);

        pageRanksOrderedRdd.saveAsTextFile("sparkJavaOutput_5");

        sc.stop();
    }

}
