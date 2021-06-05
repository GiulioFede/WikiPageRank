import dataModel.Pair;
import dataModel.Rank;
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
import java.util.ArrayList;



import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class WikiPageRank {

    private static ArrayList<String> outlinks = new ArrayList<>();
    private static String title;

    public static void main(String[] args) {

        SparkConf configuration = new SparkConf().setAppName("WikiPageRank").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(configuration);
        sc.setLogLevel("ERROR");

        System.out.println("::::::::::::::: Wiki Page Rank :::::::::::::::");

        //load wiki
        JavaRDD<String> wiki_rdd = sc.textFile("hdfs://namenode:9820/user/hadoop/input/wiki-micro.txt");

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

        //count number of page
        long numberOfPages = titles_rdd.count();

        //initialize each title with first rank (1/n)
        JavaPairRDD<String,Double> ranks = titles_rdd.mapValues(new Function<ArrayList<String>, Double>() {
            @Override
            public Double call(ArrayList<String> strings) throws Exception {

                return (1.0/numberOfPages);
            }
        });

        //loop
        for(int i =0; i<10; i++){
            JavaPairRDD<String,Double> contributions = titles_rdd.join(ranks).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<ArrayList<String>, Double>>, String, Double>() {
                @Override
                public Iterable<Tuple2<String, Double>> call(Tuple2<String, Tuple2<ArrayList<String>, Double>> tuple) throws Exception {
                    ArrayList<String> tuple_outlinks = tuple._2._1;
                    int num_outlinks = tuple_outlinks.size();
                    ArrayList<Tuple2<String,Double>> list = new ArrayList<>();
                    list.add(new Tuple2<String, Double>(tuple._1,0.0));
                    for(int j=0; j<num_outlinks;j++)
                        list.add(new Tuple2(tuple_outlinks.get(j),tuple._2._2/num_outlinks));

                    return list;
                }
            });

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
        }

        //sorting
        List<Tuple2<String,Double>> pageRanksOrdered = ranks.takeOrdered(((int)(ranks.count())), new Comparator<Tuple2<String, Double>>() {
            @Override
            public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                if(o2._2>o1._2)
                    return 1;
                else if (o2._2 == o1._2)
                    return 0;
                else
                    return -1;
            }
        });

        //save
        JavaRDD<Tuple2<String,Double>> pageRanksOrderedRdd = sc.parallelize(pageRanksOrdered);
        pageRanksOrderedRdd.saveAsTextFile("sparkJavaOutput.txt");

        sc.stop();
    }

    private static Double computeNewRank(Double lastRank, long numberOfPages){
        return (0.15*(1/numberOfPages) + 0.85*lastRank);
    }

}