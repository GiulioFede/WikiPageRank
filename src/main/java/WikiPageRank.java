import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class WikiPageRank {

    public static void main(String[] args) {

        SparkConf configuration = new SparkConf().setAppName("WikiPageRank").setMaster("yarn");

        JavaSparkContext sc = new JavaSparkContext(configuration);

        System.out.println("Hello Universe");
    }
}